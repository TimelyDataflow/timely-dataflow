
/// Information about the operator being constructed
pub struct OperatorInfo {
    index: usize,
}

/// For use in generic/, not re-exported.
///
/// Constructs `OperatorInfo`.
pub fn new_operator_info(index: usize) -> OperatorInfo {
    OperatorInfo {
        index
    }
}

impl OperatorInfo {
    /// Identifier assigned to the operator being constructed (unique within a scope)
    pub fn index(&self) -> usize {
        self.index
    }
}

