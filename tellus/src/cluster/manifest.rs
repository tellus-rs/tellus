#[cfg(debug_assertions)]
use crate::sync::lock;
#[cfg(debug_assertions)]
use std::{
    any::{self, TypeId},
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

/// A message type named for the cluster. Discovery keys, topics and serialized references carry
/// the manifest instead of the Rust type, so a lookup or a resolution as another type is refused
/// rather than answered with a reference which drops every message told to it.
///
/// A manifest must be unique per type across the cluster. Every node must use a compatible
/// payload schema for it. Collisions across nodes are unsupported: the receiving node decodes a
/// payload with its own type for the manifest. Two different types with compatible serialized
/// layouts hence decode, and the message delivered is silently misinterpreted; incompatible
/// layouts are dead-lettered. Debug builds panic when two types of one process claim the same
/// manifest, which is the only collision detected.
pub trait Manifest {
    /// The stable name of the type across the cluster, independent of the Rust type: it must not
    /// change with a type rename or move, since nodes built from other code compare it.
    const MANIFEST: &'static str;
}

#[cfg(test)]
impl Manifest for u32 {
    const MANIFEST: &'static str = "u32";
}

#[cfg(test)]
impl Manifest for u64 {
    const MANIFEST: &'static str = "u64";
}

#[cfg(test)]
impl Manifest for String {
    const MANIFEST: &'static str = "string";
}

#[cfg(debug_assertions)]
pub(crate) fn claim<M>()
where
    M: Manifest + 'static,
{
    static CLAIMS: LazyLock<Mutex<HashMap<&'static str, (TypeId, &'static str)>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    let (type_id, type_name) = *lock(&CLAIMS)
        .entry(M::MANIFEST)
        .or_insert((TypeId::of::<M>(), any::type_name::<M>()));
    assert!(
        type_id == TypeId::of::<M>(),
        "manifest {} claimed by both {type_name} and {}",
        M::MANIFEST,
        any::type_name::<M>()
    );
}

#[cfg(not(debug_assertions))]
pub(crate) fn claim<M>()
where
    M: Manifest + 'static,
{
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use crate::cluster::manifest::{Manifest, claim};

    struct First;

    impl Manifest for First {
        const MANIFEST: &'static str = "tests.collision";
    }

    struct Second;

    impl Manifest for Second {
        const MANIFEST: &'static str = "tests.collision";
    }

    struct Repeated;

    impl Manifest for Repeated {
        const MANIFEST: &'static str = "tests.repeated";
    }

    #[test]
    #[should_panic(expected = "manifest tests.collision claimed by both")]
    fn two_types_claiming_one_manifest_panic() {
        claim::<First>();
        claim::<Second>();
    }

    #[test]
    fn one_type_claims_its_manifest_repeatedly() {
        claim::<Repeated>();
        claim::<Repeated>();
    }
}
