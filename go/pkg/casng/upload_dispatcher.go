package casng

// dispatcher receives digested blobs and forwards them to the uploader or back to the requester in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per requester and notifying requesters when all of their requests are completed.
func (u *uploader) dispatcher(queryCh chan<- missingBlobRequest, queryResCh <-chan MissingBlobsResponse) {
}
