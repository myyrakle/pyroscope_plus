package querybackend

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/grafana/dskit/runutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	metastorev1 "github.com/grafana/pyroscope/api/gen/proto/go/metastore/v1"
	queryv1 "github.com/grafana/pyroscope/api/gen/proto/go/query/v1"
	"github.com/grafana/pyroscope/v2/pkg/block"
	"github.com/grafana/pyroscope/v2/pkg/block/metadata"
	"github.com/grafana/pyroscope/v2/pkg/model"
	"github.com/grafana/pyroscope/v2/pkg/model/symbolref"
	parquetquery "github.com/grafana/pyroscope/v2/pkg/phlaredb/query"
	v1 "github.com/grafana/pyroscope/v2/pkg/phlaredb/schemas/v1"
	"github.com/grafana/pyroscope/v2/pkg/phlaredb/symdb"
)

// defaultSymbolRefUnresolvedCap bounds the distinct unresolved (build ID,
// address) entries a single dataset's SymbolRefTree call may intern (see
// symdb.WithResolverSymbolRefCap); beyond it, locations render inline
// fallback names so the tree stays bounded. The value was chosen
// empirically from the worst-case rebuild cost measured for this path.
const defaultSymbolRefUnresolvedCap = 1_000_000

func init() {
	registerQueryType(
		queryv1.QueryType_QUERY_TREE,
		queryv1.ReportType_REPORT_TREE,
		queryTree,
		newTreeAggregator,
		false,
		[]block.Section{
			block.SectionTSDB,
			block.SectionProfiles,
			block.SectionSymbols,
		}...,
	)
}

func queryTree(q *queryContext, query *queryv1.Query) (*queryv1.Report, error) {
	// Mutually exclusive: no public RPC sets both, so reject an internal
	// query plan that does rather than silently apply one and drop the other.
	if query.Tree.SymbolRefs && query.Tree.FullSymbols {
		return nil, fmt.Errorf("symbol_refs and full_symbols cannot be combined")
	}

	otelSpan := trace.SpanFromContext(q.ctx)

	var profileOpts []profileIteratorOption
	if len(query.Tree.ProfileIdSelector) > 0 {
		opt, err := withProfileIDSelector(query.Tree.ProfileIdSelector...)
		if err != nil {
			return nil, err
		}
		profileOpts = append(profileOpts, opt)
		otelSpan.SetAttributes(attribute.Int("profile_id_selector.count", len(query.Tree.ProfileIdSelector)))
		if len(query.Tree.ProfileIdSelector) <= maxProfileIDsToLog {
			otelSpan.SetAttributes(attribute.String("profile_ids", strings.Join(query.Tree.ProfileIdSelector, ",")))
		}
	}

	entries, err := profileEntryIterator(q, profileOpts...)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, entries, "failed to close profile entry iterator")

	spanSelector, err := model.NewSpanSelector(query.Tree.SpanSelector)
	if err != nil {
		return nil, err
	}

	traceSelector, err := model.NewTraceSelector(query.Tree.TraceIdSelector)
	if err != nil {
		return nil, err
	}

	// Mutually exclusive: no public RPC sets both, so reject an internal query
	// plan that does rather than silently apply one and drop the other.
	if len(spanSelector) > 0 && len(traceSelector) > 0 {
		return nil, fmt.Errorf("span_selector and trace_id_selector cannot be combined")
	}

	var columns v1.SampleColumns
	if err = columns.Resolve(q.ds.Profiles().Schema()); err != nil {
		return nil, err
	}

	indices := []int{
		columns.StacktraceID.ColumnIndex,
		columns.Value.ColumnIndex,
	}
	switch {
	case len(spanSelector) > 0:
		if !columns.HasSpanID() {
			// Block has no SpanID column: no samples can match the span selector.
			return &queryv1.Report{Tree: &queryv1.TreeReport{Query: query.Tree.CloneVT()}}, nil
		}
		indices = append(indices, columns.SpanID.ColumnIndex)
	case len(traceSelector) > 0:
		if !columns.HasTraceID() {
			// Block has no TraceID column: no samples can match the trace selector.
			return &queryv1.Report{Tree: &queryv1.TreeReport{Query: query.Tree.CloneVT()}}, nil
		}
		indices = append(indices, columns.TraceID.ColumnIndex)
	}

	resolverOptions := []symdb.ResolverOption{
		symdb.WithResolverMaxNodes(query.Tree.MaxNodes),
	}
	if query.Tree.StackTraceSelector != nil {
		resolverOptions = append(resolverOptions, symdb.WithResolverStackTraceSelector(query.Tree.StackTraceSelector))
	}
	if query.Tree.SymbolRefs {
		resolverOptions = append(resolverOptions, symdb.WithResolverSymbolRefCap(defaultSymbolRefUnresolvedCap))
	}

	profiles := parquetquery.NewRepeatedRowIterator(q.ctx, entries, q.ds.Profiles().RowGroups(), indices...)
	defer runutil.CloseWithErrCapture(&err, profiles, "failed to close profile stream")

	resolver := symdb.NewResolver(q.ctx, q.ds.Symbols(), resolverOptions...)
	defer resolver.Release()

	switch {
	case len(spanSelector) > 0:
		for profiles.Next() {
			p := profiles.At()
			resolver.AddSamplesWithSpanSelectorFromParquetRow(
				p.Row.Partition,
				p.Values[0],
				p.Values[1],
				p.Values[2],
				spanSelector,
			)
		}
	case len(traceSelector) > 0:
		for profiles.Next() {
			p := profiles.At()
			resolver.AddSamplesWithTraceSelectorFromParquetRow(
				p.Row.Partition,
				p.Values[0],
				p.Values[1],
				p.Values[2],
				traceSelector,
			)
		}
	default:
		for profiles.Next() {
			p := profiles.At()
			resolver.AddSamplesFromParquetRow(p.Row.Partition, p.Values[0], p.Values[1])
		}
	}

	if err = profiles.Err(); err != nil {
		return nil, err
	}

	// output full pprof tree if that's requested
	if query.Tree.FullSymbols {
		tree, symbolBuilder, err := resolver.LocationRefNameTree()
		if err != nil {
			return nil, err
		}
		resp := &queryv1.Report{
			Tree: &queryv1.TreeReport{
				Query:   query.Tree.CloneVT(),
				Tree:    tree.Bytes(query.Tree.GetMaxNodes(), symbolBuilder.KeepSymbol),
				Symbols: new(queryv1.TreeSymbols),
			},
		}
		symbolBuilder.Build(resp.Tree.Symbols)
		return resp, nil
	}

	// A dataset not labeled unsymbolized has nothing to defer resolution
	// for: keep the existing FunctionName path unconditionally, same as a
	// non-symbol-ref query.
	if query.Tree.SymbolRefs && datasetUnsymbolized(q.obj.Metadata(), q.ds.Metadata()) {
		return queryTreeSymbolRefs(q, query, resolver)
	}

	tree, err := resolver.Tree()
	if err != nil {
		return nil, err
	}

	resp := &queryv1.Report{
		Tree: &queryv1.TreeReport{
			Query: query.Tree.CloneVT(),
			Tree:  tree.Bytes(query.Tree.GetMaxNodes(), nil),
		},
	}
	return resp, nil
}

// queryTreeSymbolRefs builds the report for a dataset labeled unsymbolized:
// the resolver defers resolution of frames it cannot symbolize locally into
// a SymbolRefTable instead of dropping or approximating them. If none of
// the selected samples actually resolved to an unresolved location, the
// dataset falls back to the plain FunctionName path, since there is
// nothing left to defer.
func queryTreeSymbolRefs(q *queryContext, query *queryv1.Query, resolver *symdb.Resolver) (*queryv1.Report, error) {
	tree, rb, exceeded, err := resolver.SymbolRefTree()
	if err != nil {
		return nil, err
	}
	if exceeded > 0 {
		q.metrics.symbolRefUnresolvedCapExceeded.Add(float64(exceeded))
	}

	// tree.Bytes must run before rb.Build: Build only reports the refs
	// KeepRef (invoked from Bytes as it marshals) has observed as reachable.
	treeBytes := tree.Bytes(0, rb.KeepRef)
	symbolRefs := new(queryv1.SymbolRefTable)
	rb.Build(symbolRefs)

	if len(symbolRefs.UnresolvedBuildId) == 0 {
		// Labeled unsymbolized, but nothing the query selected actually
		// resolved to an unresolved location (e.g. rollout skew, or a
		// selector that only matched already-resolved stacks): convert back
		// to a plain, truncatable FunctionName report via the same rb,
		// rather than re-resolving the stack traces from scratch.
		plain := locationRefTreeToFunctionNameTree(tree, rb)
		return &queryv1.Report{
			Tree: &queryv1.TreeReport{
				Query: query.Tree.CloneVT(),
				Tree:  plain.Bytes(query.Tree.GetMaxNodes(), nil),
			},
		}, nil
	}

	return &queryv1.Report{
		Tree: &queryv1.TreeReport{
			Query:      query.Tree.CloneVT(),
			Tree:       treeBytes,
			SymbolRefs: symbolRefs,
		},
	}, nil
}

// datasetUnsymbolized reports whether any of the dataset's label sets
// carries __unsymbolized__="true". A compacted dataset accumulates the
// label sets of all its sources, so every set must be checked, not just
// the first.
func datasetUnsymbolized(md *metastorev1.BlockMeta, ds *metastorev1.Dataset) bool {
	pairs := metadata.LabelPairs(ds.Labels)
	for pairs.Next() {
		p := pairs.At()
		for k := 0; k+1 < len(p); k += 2 {
			n, v := p[k], p[k+1]
			if n < 0 || int(n) >= len(md.StringTable) || v < 0 || int(v) >= len(md.StringTable) {
				continue
			}
			if md.StringTable[n] == metadata.LabelNameUnsymbolized && md.StringTable[v] == "true" {
				return true
			}
		}
	}
	return false
}

type treeAggregator struct {
	init  sync.Once
	query *queryv1.TreeQuery
	tree  *model.TreeMerger[model.FunctionName, model.FunctionNameI]

	lrTree       *model.TreeMerger[model.LocationRefName, model.LocationRefNameI]
	symbolLock   sync.Mutex
	symbolMerger *symdb.SymbolMerger

	symbolRefTable *symbolref.Table
	symbolRefTree  *model.TreeMerger[model.LocationRefName, model.LocationRefNameI]
}

func newTreeAggregator(*queryv1.InvokeRequest) aggregator { return new(treeAggregator) }

func (a *treeAggregator) aggregate(report *queryv1.Report) error {
	r := report.Tree
	switch {
	case r.Query.SymbolRefs:
		a.init.Do(func() {
			a.symbolRefTable = symbolref.NewTable()
			a.symbolRefTree = model.NewTreeMerger[model.LocationRefName, model.LocationRefNameI]()
			a.query = r.Query.CloneVT()
		})
		if r.SymbolRefs == nil {
			// A partial from a dataset that took the plain FunctionName
			// path (native or degenerate, see queryTree): absorb it into
			// the shared ref space instead of merging it as a plain tree.
			absorbed, err := absorbPlainTree(a.symbolRefTable, r.Tree)
			if err != nil {
				return err
			}
			a.symbolRefTree.MergeTree(absorbed)
			return nil
		}
		remap, err := a.symbolRefTable.Add(r.SymbolRefs)
		if err != nil {
			return err
		}
		return a.symbolRefTree.MergeTreeBytes(r.Tree, model.WithTreeMergeFormatNodeNames(remap))

	case r.Query.FullSymbols:
		a.init.Do(func() {
			a.lrTree = model.NewTreeMerger[model.LocationRefName, model.LocationRefNameI]()
			a.query = r.Query.CloneVT()
			a.symbolMerger = symdb.NewSymbolMerger()
		})
		a.symbolLock.Lock()
		defer a.symbolLock.Unlock()
		adder, err := a.symbolMerger.Add(r.Symbols)
		if err != nil {
			return err
		}
		return a.lrTree.MergeTreeBytes(r.Tree, model.WithTreeMergeFormatNodeNames(adder))

	default:
		a.init.Do(func() {
			a.tree = model.NewTreeMerger[model.FunctionName, model.FunctionNameI]()
			a.query = r.Query.CloneVT()
		})
		return a.tree.MergeTreeBytes(r.Tree)
	}
}

func (a *treeAggregator) build() *queryv1.Report {
	result := &queryv1.Report{
		Tree: &queryv1.TreeReport{
			Query: a.query,
		},
	}

	switch {
	case a.query.SymbolRefs:
		if !a.symbolRefTable.HasUnresolved() {
			// Every partial absorbed cleanly (native datasets, or degenerate
			// ones): there is nothing left to defer, so collapse back to a
			// plain, truncatable FunctionName report instead of forcing the
			// SymbolRefTable format on a query that never needed it. This
			// keeps a symbol_refs query over an otherwise fully-symbolized
			// tenant no more expensive than a plain one, and matches
			// queryTree's own degenerate case (see queryTreeSymbolRefs).
			plain := locationRefTreeToFunctionNameTree(a.symbolRefTree.Tree(), a.symbolRefTable.ResultBuilder())
			result.Tree.Tree = plain.Bytes(a.query.GetMaxNodes(), nil)
			return result
		}
		// A tree with unresolved entries must not be truncated before those
		// are resolved: truncation is deferred to a later symbolref.Rebuild
		// call, once resolution has happened.
		rb := a.symbolRefTable.ResultBuilder()
		result.Tree.Tree = a.symbolRefTree.Tree().Bytes(0, rb.KeepRef)
		result.Tree.SymbolRefs = new(queryv1.SymbolRefTable)
		rb.Build(result.Tree.SymbolRefs)
		return result

	case a.query.FullSymbols:
		builder := a.symbolMerger.ResultBuilder()
		result.Tree.Tree = a.lrTree.Tree().Bytes(a.query.GetMaxNodes(), builder.KeepSymbol)
		result.Tree.Symbols = new(queryv1.TreeSymbols)
		builder.Build(result.Tree.Symbols)
		return result

	default:
		result.Tree.Tree = a.tree.Tree().Bytes(a.query.GetMaxNodes(), nil)
		return result
	}
}

// absorbPlainTree unmarshals a plain FunctionNameTree and re-inserts every
// stack into a LocationRefNameTree via dst.InternName. It drops the
// spurious root-most "" frame the tree carries after UnmarshalTree, and
// maps model.OtherFunctionName to model.OtherLocationRef directly rather
// than through InternName, since both are the truncation sentinel in
// their respective node-name spaces.
func absorbPlainTree(dst *symbolref.Table, plainTreeBytes []byte) (*model.LocationRefNameTree, error) {
	plain, err := model.UnmarshalTree[model.FunctionName, model.FunctionNameI](plainTreeBytes)
	if err != nil {
		return nil, err
	}
	absorbed := new(model.LocationRefNameTree)
	plain.IterateStacks(func(_ model.FunctionName, self int64, stack []model.FunctionName) {
		slices.Reverse(stack)
		if len(stack) > 0 && stack[0] == "" {
			stack = stack[1:]
		}
		refs := make([]model.LocationRefName, len(stack))
		for i, n := range stack {
			if n == model.OtherFunctionName {
				refs[i] = model.OtherLocationRef
				continue
			}
			refs[i] = dst.InternName(string(n))
		}
		absorbed.InsertStack(self, refs...)
	})
	return absorbed, nil
}

// stackToInsert is a root-first stack of table refs, paired with its self
// value, collected from a tree so an intermediate ResultBuilder pass can
// run before it is known how any given ref renders as a name.
type stackToInsert struct {
	self  int64
	stack []model.LocationRefName
}

// locationRefTreeToFunctionNameTree converts tree into a FunctionNameTree,
// resolving every ref through rb. The caller must ensure rb's table has no
// unresolved entries reachable from tree: any such ref renders as the
// empty string, since there is no name to fall back to. rb may already
// have observed refs from an earlier KeepRef/Build pass (e.g. a prior
// marshal of the same tree): KeepRef is idempotent, so this only adds to
// rb's state, never invalidates it.
func locationRefTreeToFunctionNameTree(tree *model.LocationRefNameTree, rb *symbolref.ResultBuilder) *model.FunctionNameTree {
	var stacks []stackToInsert
	tree.IterateStacks(func(_ model.LocationRefName, self int64, stack []model.LocationRefName) {
		cp := append([]model.LocationRefName(nil), stack...)
		slices.Reverse(cp)
		if len(cp) > 0 && cp[0] == 0 {
			// The marshal format's spurious root-most frame: ref 0 is
			// reserved and never a real name (see symbolref.NewTable).
			cp = cp[1:]
		}
		stacks = append(stacks, stackToInsert{self: self, stack: cp})
	})

	kept := make([][]model.LocationRefName, len(stacks))
	for i, e := range stacks {
		k := make([]model.LocationRefName, len(e.stack))
		for j, ref := range e.stack {
			k[j] = rb.KeepRef(ref)
		}
		kept[i] = k
	}
	pb := new(queryv1.SymbolRefTable)
	rb.Build(pb)

	out := new(model.FunctionNameTree)
	for i, e := range stacks {
		names := make([]model.FunctionName, len(e.stack))
		for j, ref := range e.stack {
			if ref == model.OtherLocationRef {
				names[j] = model.OtherFunctionName
				continue
			}
			if idx := kept[i][j]; idx >= 0 && int(idx) < len(pb.Names) {
				names[j] = model.FunctionName(pb.Names[idx])
			}
		}
		out.InsertStack(e.self, names...)
	}
	return out
}
