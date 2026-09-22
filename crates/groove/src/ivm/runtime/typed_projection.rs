//! Reusable, source-independent projection compilation.
//!
//! Keys include the exact descriptor (including field identities and enum
//! registries) and every expression, literal, name and output identity. A plan
//! contains no node IDs, source handles, row data, authorization or retainers.
//! Reusing it therefore cannot reuse an input scope or keep a graph alive.

use super::*;
use std::cell::OnceCell;

const PLAN_SLOTS: usize = 512;
// Bound retained key payload as well as entry count. This is a hash-stream
// budget, not a heap accounting measurement; oversized definitions still work
// but are not retained. The descriptor itself is an interned, immutable handle.
const MAX_KEY_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug)]
pub(super) struct ProjectionPlan {
    input: RecordDescriptor,
    fields: Vec<ProjectField>,
    pub(super) output: RecordDescriptor,
    expressions: OnceCell<Vec<ProjectionExpr>>,
}

impl ProjectionPlan {
    pub(super) fn expressions(&self) -> Result<&[ProjectionExpr], IvmRuntimeError> {
        if let Some(expressions) = self.expressions.get() {
            return Ok(expressions);
        }
        // Keep output inference and expression validation at their existing
        // boundaries: inferring an output alone must not validate execution.
        let expressions = self
            .fields
            .iter()
            .map(|field| {
                project_field_expr(&self.input, field).map(|expression| ProjectionExpr {
                    expression,
                    output_name: Some(field.output_name.clone()),
                    output_identity: field.output_identity.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let _ = self.expressions.set(expressions);
        Ok(self
            .expressions
            .get()
            .expect("projection expressions initialized"))
    }
}

#[derive(Clone, Debug)]
pub(super) struct ProjectionPlans {
    // Direct mapping bounds metadata and makes eviction constant-time. A hash
    // collision is a miss, never evidence of semantic equivalence.
    slots: Vec<Option<Rc<ProjectionPlan>>>,
    #[cfg(test)]
    pub(super) hits: usize,
}

impl Default for ProjectionPlans {
    fn default() -> Self {
        Self {
            slots: vec![None; PLAN_SLOTS],
            #[cfg(test)]
            hits: 0,
        }
    }
}

#[derive(Default)]
struct KeyHasher {
    inner: DefaultHasher,
    bytes: usize,
}

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.inner.finish()
    }
    fn write(&mut self, bytes: &[u8]) {
        self.bytes = self.bytes.saturating_add(bytes.len());
        self.inner.write(bytes);
    }
}

impl ProjectionPlans {
    pub(super) fn get(
        &mut self,
        input: RecordDescriptor,
        fields: &[ProjectField],
    ) -> Result<Rc<ProjectionPlan>, IvmRuntimeError> {
        let mut hash = KeyHasher::default();
        input.hash(&mut hash);
        fields.hash(&mut hash);
        let slot = hash.finish() as usize % PLAN_SLOTS;
        if let Some(plan) = &self.slots[slot]
            && plan.input == input
            && plan.fields == fields
        {
            #[cfg(test)]
            {
                self.hits += 1;
            }
            return Ok(Rc::clone(plan));
        }
        let output = project_descriptor(&input, fields)?;
        let plan = Rc::new(ProjectionPlan {
            input,
            fields: fields.to_vec(),
            output,
            expressions: OnceCell::new(),
        });
        if hash.bytes <= MAX_KEY_BYTES {
            self.slots[slot] = Some(Rc::clone(&plan));
        }
        Ok(plan)
    }
}

impl IvmRuntime {
    pub(super) fn projection_plan(
        &self,
        input: RecordDescriptor,
        fields: &[ProjectField],
    ) -> Result<Rc<ProjectionPlan>, IvmRuntimeError> {
        self.projection_plans.borrow_mut().get(input, fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal evidence is necessary to distinguish recompiling a correct
    // projection from reusing its typed plan. Public runtime tests above cover
    // actual rows, distinct sources/literals and descriptor changes.
    #[test]
    fn typed_projection_plan_reuses_exact_definitions_and_checks_hash_collisions() {
        let mut plans = ProjectionPlans::default();
        let input = RecordDescriptor::new([("id", ValueType::U64)]);
        let fields = [ProjectField::named("id")];
        let first = plans.get(input, &fields).unwrap();
        let expressions = first.expressions().unwrap().as_ptr();
        let second = plans.get(input, &fields).unwrap();
        assert!(Rc::ptr_eq(&first, &second));
        assert_eq!(expressions, second.expressions().unwrap().as_ptr());

        let renamed = [ProjectField::renamed("id", "other")];
        let incompatible = plans.get(input, &renamed).unwrap();
        // Force even the exact target bucket to hold a different definition.
        // The comparison must reject it, independently of hash behavior.
        plans.slots.fill(Some(incompatible));
        let restored = plans.get(input, &fields).unwrap();
        assert_eq!(restored.output, first.output);
        assert_eq!(
            restored.expressions().unwrap(),
            first.expressions().unwrap()
        );
        assert!(!Rc::ptr_eq(&first, &restored));

        let different_identity = [ProjectField::named_with_identity(
            "id",
            crate::records::FieldIdentity::Slot(42),
        )];
        assert_ne!(
            plans.get(input, &different_identity).unwrap().output,
            first.output
        );
        let oversized = [ProjectField::literal(
            "large",
            Value::String("x".repeat(MAX_KEY_BYTES + 1)),
        )];
        let a = plans.get(input, &oversized).unwrap();
        let b = plans.get(input, &oversized).unwrap();
        assert!(!Rc::ptr_eq(&a, &b));
        assert_eq!(a.output, b.output);
    }
}
