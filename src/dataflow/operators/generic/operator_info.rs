
/// Information about the operator being constructed
pub struct OperatorInfo {
    local_id: usize,
    global_id: usize,
}

/// For use in generic/, not re-exported.
///
/// Constructs `OperatorInfo`.
pub fn new_operator_info(local_id: usize, global_id: usize) -> OperatorInfo {
    OperatorInfo {
        local_id,
        global_id,
    }
}

impl OperatorInfo {
    /// Scope-local index assigned to the operator being constructed.
    pub fn index(&self) -> usize { self.local_id }
    /// Worker-unique identifier.
    pub fn global(&self) -> usize { self.global_id }
}

