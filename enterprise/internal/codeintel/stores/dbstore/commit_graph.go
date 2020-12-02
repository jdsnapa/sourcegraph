package dbstore

import (
	"sync"

	"github.com/sourcegraph/sourcegraph/enterprise/internal/codeintel/gitserver"
)

// calculateVisibleUploads transforms the given commit graph and the set of LSIF uploads
// defined on each commit with LSIF upload into a map from a commit to the set of uploads
// which are visible from that commit.
func calculateVisibleUploads(commitGraph *gitserver.CommitGraph, commitGraphView *CommitGraphView) (map[string][]UploadMeta, error) {
	graph := commitGraph.Graph()
	order := commitGraph.Order()

	// Calculate mapping from commits to their children
	reverseGraph := reverseGraph(graph)

	var wg sync.WaitGroup
	wg.Add(2)

	// ancestorVisibleUploads maps commits to the set of uploads visible by looking up the ancestor
	// paths of that commit. This map is populated by first inserting each upload at the commit
	// where it is defined. Then we populate the remaining data by walking the graph in topological
	// order (parents before children), and "push" each visible upload down descendant paths. At each
	// commit, if multiple uploads with the same root and indexer are visible, the one with the minmum
	// distance from the source commit will be used.
	ancestorVisibleUploads := make(map[string]map[string]UploadMeta, len(order))

	go func() {
		defer wg.Done()

		for _, commit := range order {
			ancestorVisibleUploads[commit] = map[string]UploadMeta{}

			for _, upload := range commitGraphView.Meta[commit] {
				ancestorVisibleUploads[commit][commitGraphView.Tokens[upload.UploadID]] = UploadMeta{
					UploadID: upload.UploadID,
					Flags:    upload.Flags | FlagAncestorVisible,
				}
			}
		}

		for _, commit := range order {
			for _, parent := range graph[commit] {
				for _, upload := range ancestorVisibleUploads[parent] {
					addUploadMeta(ancestorVisibleUploads, commitGraphView, commit, UploadMeta{
						UploadID: upload.UploadID,
						Flags:    upload.Flags + 1,
					})
				}
			}
		}
	}()

	// descendantVisibleUploads maps commits to the set of uploads visible by looking down the
	// descendant paths of that commit. This map is populated by first inserting each upload at the
	// commit where it is defined. Then we populate the remaining data by walking the graph in reverse
	// topological order (children before parents), and "push" each visible upload up ancestor paths.
	// At each  commit, if multiple uploads with the same root and indexer are visible, the one with
	// the minmum  distance from the source commit will be used.
	descendantVisibleUploads := make(map[string]map[string]UploadMeta, len(order))

	go func() {
		defer wg.Done()

		for _, commit := range order {
			descendantVisibleUploads[commit] = map[string]UploadMeta{}

			for _, upload := range commitGraphView.Meta[commit] {
				descendantVisibleUploads[commit][commitGraphView.Tokens[upload.UploadID]] = UploadMeta{
					UploadID: upload.UploadID,
					Flags:    upload.Flags &^ FlagAncestorVisible,
				}
			}
		}

		for i := len(order) - 1; i >= 0; i-- {
			commit := order[i]

			for _, child := range reverseGraph[commit] {
				for _, upload := range descendantVisibleUploads[child] {
					addUploadMeta(descendantVisibleUploads, commitGraphView, commit, UploadMeta{
						UploadID: upload.UploadID,
						Flags:    upload.Flags + 1,
					})
				}
			}
		}
	}()

	wg.Wait()

	combined := make(map[string][]UploadMeta, len(order))
	for _, commit := range order {
		capacity := len(ancestorVisibleUploads[commit])
		if temp := len(descendantVisibleUploads[commit]); capacity < temp {
			capacity = temp
		}
		uploads := make([]UploadMeta, 0, capacity)

		for token, ancestorUpload := range ancestorVisibleUploads[commit] {
			if descendantUpload, ok := descendantVisibleUploads[commit][token]; ok {
				if replaces(descendantUpload, ancestorUpload) {
					ancestorUpload.Flags |= FlagOverwritten
					uploads = append(uploads, descendantUpload)
				}
			}

			uploads = append(uploads, ancestorUpload)
		}

		for token, upload2 := range descendantVisibleUploads[commit] {
			if _, ok := ancestorVisibleUploads[commit][token]; !ok {
				uploads = append(uploads, upload2)
			}
		}

		combined[commit] = uploads
	}

	return combined, nil
}

// reverseGraph returns the reverse of the given graph by flipping all the edges.
func reverseGraph(graph map[string][]string) map[string][]string {
	reverse := make(map[string][]string, len(graph))
	for child := range graph {
		reverse[child] = nil
	}

	for child, parents := range graph {
		for _, parent := range parents {
			reverse[parent] = append(reverse[parent], child)
		}
	}

	return reverse
}

func addUploadMeta(uploads map[string]map[string]UploadMeta, commitGraphView *CommitGraphView, commit string, upload UploadMeta) {
	uploadsByToken, ok := uploads[commit]
	if !ok {
		uploadsByToken = map[string]UploadMeta{}
		uploads[commit] = uploadsByToken
	}

	token := commitGraphView.Tokens[upload.UploadID]

	if currentUpload, ok := uploadsByToken[token]; !ok || replaces(upload, currentUpload) {
		uploadsByToken[token] = upload
	}
}

func replaces(u1, u2 UploadMeta) bool {
	d1 := u1.Flags & MaxDistance
	d2 := u2.Flags & MaxDistance

	return d1 < d2 || (d1 == d2 && u1.UploadID < u2.UploadID)
}
