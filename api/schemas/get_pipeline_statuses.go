package schemas

// GetPipelineStatusesRequest is empty: querying pipeline statuses takes no
// parameters.
type GetPipelineStatusesRequest struct{}

// PipelineStatus is the convergence state of one running pipeline instance.
type PipelineStatus struct {
	// Pipeline is the configured pipeline name.
	Pipeline string `json:"pipeline"`
	// Phase is the convergence phase: "running", "pending" or "error".
	Phase string `json:"phase"`
	// ActiveVersion is the configuration version of the build currently
	// serving records; 0 when no build ever succeeded.
	ActiveVersion uint64 `json:"active_version"`
	// DesiredVersion is the latest configuration version observed for the
	// pipeline, which the runner is converging towards.
	DesiredVersion uint64 `json:"desired_version"`
	// Serving reports whether an active build exists and can process records,
	// including when an older build is kept after a failed candidate.
	Serving bool `json:"serving"`
	// Error carries the last candidate build failure message, if any.
	Error string `json:"error,omitempty"`
}

// GetPipelineStatusesResponse carries the convergence state of every pipeline.
type GetPipelineStatusesResponse struct {
	// Success reports whether the status query completed.
	Success bool `json:"success"`
	// Statuses holds one entry per known pipeline.
	Statuses []PipelineStatus `json:"statuses"`
}
