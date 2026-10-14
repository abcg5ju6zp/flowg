package pipelines

// PipelinePhase is the convergence phase of one pipeline instance relative to
// the configuration version it is expected to run.
type PipelinePhase string

const (
	// PhaseRunning: a build matching the desired version is serving records.
	PhaseRunning PipelinePhase = "running"
	// PhasePending: a candidate build for a newer version is in flight while an
	// older build may already serve records.
	PhasePending PipelinePhase = "pending"
	// PhaseError: the latest desired version failed to build; an older active
	// build may still be serving records (Serving=true in the status).
	PhaseError PipelinePhase = "error"
)

// PipelineStatus is a point-in-time, concurrency-safe snapshot of one pipeline
// instance's convergence state, as returned by Runner.Status.
type PipelineStatus struct {
	// Name is the configured pipeline name.
	Name string
	// Phase is running, pending or error.
	Phase PipelinePhase
	// ActiveVersion is the configuration version compiled into the build that
	// currently serves records; 0 when no build ever succeeded.
	ActiveVersion uint64
	// DesiredVersion is the latest configuration version observed for this
	// pipeline; the runner is converging (or failed to converge) towards it.
	DesiredVersion uint64
	// Serving reports whether an active build exists and can process records,
	// including when the latest candidate failed and an older build is kept.
	Serving bool
	// Error is the failure message of the last candidate build at the desired
	// version; empty when that build is running or pending.
	Error string
}

// pipelineState is the mutable convergence state of one pipeline, owned and
// touched exclusively by the runner actor goroutine.
type pipelineState struct {
	name string

	// active is the last build that compiled and initialized successfully; it
	// keeps serving records while newer candidates fail or build.
	active        *Pipeline
	activeVersion uint64

	// desiredVersion is the highest configuration version observed for this
	// pipeline.
	desiredVersion uint64

	// failedVersion is the version of the latest failed candidate at the
	// desired version (0 when the desired version is healthy or unknown).
	failedVersion uint64
	lastError     string

	// building counts candidate builds currently in flight.
	building int
}

// phase derives the externally visible phase from the state fields.
func (st *pipelineState) phase() PipelinePhase {
	switch {
	case st.building > 0:
		return PhasePending

	case st.active != nil &&
		st.activeVersion >= st.desiredVersion &&
		(st.lastError == "" || st.failedVersion < st.desiredVersion):
		return PhaseRunning

	default:
		return PhaseError
	}
}

// snapshot copies the state into an exported PipelineStatus.
func (st *pipelineState) snapshot() PipelineStatus {
	status := PipelineStatus{
		Name:           st.name,
		Phase:          st.phase(),
		ActiveVersion:  st.activeVersion,
		DesiredVersion: st.desiredVersion,
		Serving:        st.active != nil,
	}

	// while a newer candidate is building, a previous failure is no longer the
	// current verdict
	if status.Phase != PhasePending {
		status.Error = st.lastError
	}

	return status
}
