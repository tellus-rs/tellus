use crate::backoff::Backoff;
use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

/// Configuration for an [Actor].
///
/// [Actor]: crate::Actor
#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(default, deny_unknown_fields)
)]
pub struct ActorConfig {
    /// The capacity of the actor's mailbox. Defaults to [MailboxCapacity::Unbounded].
    pub mailbox_capacity: MailboxCapacity,

    /// The strategy deciding what happens if the actor has failed. Defaults to
    /// [SupervisionStrategy::Stop].
    pub supervision_strategy: SupervisionStrategy,
}

impl ActorConfig {
    /// This config, sizing the actor's mailbox by the given [MailboxCapacity].
    pub fn with_mailbox_capacity(self, mailbox_capacity: MailboxCapacity) -> Self {
        Self {
            mailbox_capacity,
            ..self
        }
    }

    /// This config, deciding by the given [SupervisionStrategy] what happens if the actor has
    /// failed.
    pub fn with_supervision_strategy(self, supervision_strategy: SupervisionStrategy) -> Self {
        Self {
            supervision_strategy,
            ..self
        }
    }
}

/// The capacity of an actor's mailbox.
///
/// Prefer [Bounded](Self::Bounded), sized by the number of messages the protocol allows in flight
/// or, failing that, by the largest burst the actor should absorb. The default is
/// [Unbounded](Self::Unbounded) only because no capacity fits every actor.
#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum MailboxCapacity {
    /// Messages are never dropped, but an actor which cannot keep up grows its mailbox without
    /// limit.
    #[default]
    Unbounded,

    /// Messages sent to a full mailbox are dropped and logged as dead letters. A full mailbox still
    /// takes terminated signals, the outputs of futures piped with [ActorContext::pipe_to_self],
    /// the replies to destinations from [ActorContext::reply_to] and what a [Host] buffered for it,
    /// so it can exceed its capacity by those.
    ///
    /// [ActorContext::pipe_to_self]: crate::ActorContext::pipe_to_self
    /// [ActorContext::reply_to]: crate::ActorContext::reply_to
    /// [Host]: crate::Host
    Bounded(NonZeroUsize),
}

/// The strategy deciding what happens to an actor that has failed.
#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(rename_all = "snake_case")
)]
pub enum SupervisionStrategy {
    /// Stop the actor.
    #[default]
    Stop,

    /// Stop the child actors and replace the current state with a newly initialized one, limited
    /// and paced by the given [RestartPolicy]. A restart cancels the actor's timers and keeps its
    /// mailbox.
    ///
    /// [Actor::init] is re-run on the same actor value: anything the actor itself carries survives
    /// the restart, and only the state is rebuilt. An event-sourced actor recovers its state by
    /// replay instead of running [Actor::init].
    ///
    /// [Actor::init]: crate::Actor::init
    Restart(RestartPolicy),
}

/// The limit and pacing for restarts, applied to failures of [Actor::receive] and [Actor::init]
/// alike, including the first initialization at spawn.
///
/// Restarts are counted: the n-th counted restart is delayed by `backoff.min() * 2^(n-1)`, capped
/// at `backoff.max()`. When the actor fails again after `max_restarts` counted restarts, it stops,
/// so persistent failure escalates to the watchers. The restart count, and with it the backoff,
/// resets once the actor has run for at least `reset_after` without failing.
///
/// [Actor::init]: crate::Actor::init
/// [Actor::receive]: crate::Actor::receive
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize),
    serde(deny_unknown_fields)
)]
pub struct RestartPolicy {
    /// The maximum number of counted restarts; one more failure stops the actor.
    pub max_restarts: NonZeroU32,

    /// The bounds pacing the counted restarts. Defaults to [Backoff::default].
    #[cfg_attr(feature = "serde", serde(default))]
    pub backoff: Backoff,

    /// Running this long without failure resets the restart count. Defaults to
    /// [RestartPolicy::DEFAULT_RESET_AFTER].
    #[cfg_attr(
        feature = "serde",
        serde(default = "default_reset_after", with = "humantime_serde")
    )]
    pub reset_after: Duration,
}

impl RestartPolicy {
    /// The `reset_after` of a policy created by [RestartPolicy::new].
    pub const DEFAULT_RESET_AFTER: Duration = Duration::from_secs(30);

    /// A policy with the given restart limit, paced by [Backoff::default] and
    /// [RestartPolicy::DEFAULT_RESET_AFTER].
    pub fn new(max_restarts: NonZeroU32) -> Self {
        Self {
            max_restarts,
            backoff: Backoff::default(),
            reset_after: Self::DEFAULT_RESET_AFTER,
        }
    }

    /// This policy, pacing the counted restarts by the given [Backoff].
    pub fn with_backoff(self, backoff: Backoff) -> Self {
        Self { backoff, ..self }
    }

    /// This policy, resetting the restart count once the actor has run this long without failing.
    pub fn with_reset_after(self, reset_after: Duration) -> Self {
        Self {
            reset_after,
            ..self
        }
    }
}

#[cfg(feature = "serde")]
fn default_reset_after() -> Duration {
    RestartPolicy::DEFAULT_RESET_AFTER
}

#[cfg(test)]
mod tests {
    use crate::{ActorConfig, Backoff, MailboxCapacity, RestartPolicy, SupervisionStrategy};
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        time::Duration,
    };

    const MIN_BACKOFF: Duration = Duration::from_millis(1);
    const MAX_BACKOFF: Duration = Duration::from_millis(10);

    /// A new policy is paced by the defaults its documentation names, so a caller only has to
    /// overwrite what it wants to differ.
    #[test]
    fn a_new_policy_is_paced_by_the_defaults() {
        let policy = RestartPolicy::new(NonZeroU32::MIN);

        assert_eq!(policy.backoff.min(), Backoff::DEFAULT_MIN);
        assert_eq!(policy.backoff.max(), Backoff::DEFAULT_MAX);
        assert_eq!(policy.reset_after, RestartPolicy::DEFAULT_RESET_AFTER);
    }

    /// Each setter overwrites its own field and leaves the others as they were.
    #[test]
    fn a_config_setter_only_overwrites_its_own_field() {
        let config = ActorConfig::default()
            .with_mailbox_capacity(MailboxCapacity::Bounded(NonZeroUsize::MIN))
            .with_supervision_strategy(SupervisionStrategy::Restart(RestartPolicy::new(
                NonZeroU32::MIN,
            )));

        assert!(matches!(
            config.mailbox_capacity,
            MailboxCapacity::Bounded(capacity) if capacity == NonZeroUsize::MIN
        ));
        assert!(matches!(
            config.supervision_strategy,
            SupervisionStrategy::Restart(_)
        ));
    }

    /// Each setter overwrites its own field and leaves the others as they were.
    #[test]
    fn a_policy_setter_only_overwrites_its_own_field() {
        let backoff = Backoff::new(MIN_BACKOFF, MAX_BACKOFF).expect("the bounds are valid");
        let policy = RestartPolicy::new(NonZeroU32::MIN)
            .with_backoff(backoff)
            .with_reset_after(Duration::ZERO);

        assert_eq!(policy.max_restarts, NonZeroU32::MIN);
        assert_eq!(policy.backoff.min(), MIN_BACKOFF);
        assert_eq!(policy.backoff.max(), MAX_BACKOFF);
        assert_eq!(policy.reset_after, Duration::ZERO);
    }

    /// The snake_case variant names, the per-field defaults and the humantime duration format are
    /// a public configuration surface; this pins it down.
    #[cfg(feature = "serde")]
    #[test]
    fn a_config_deserializes_from_its_documented_form() {
        let config = serde_json::from_str::<ActorConfig>(
            r#"{
                "mailbox_capacity": { "bounded": 42 },
                "supervision_strategy": { "restart": { "max_restarts": 3, "reset_after": "1m" } }
            }"#,
        )
        .expect("the documented config form deserializes");

        assert!(matches!(
            config.mailbox_capacity,
            MailboxCapacity::Bounded(capacity) if capacity.get() == 42
        ));

        let SupervisionStrategy::Restart(policy) = config.supervision_strategy else {
            panic!("expected a restart strategy")
        };
        assert_eq!(policy.max_restarts.get(), 3);
        assert_eq!(policy.reset_after, Duration::from_secs(60));
        assert_eq!(policy.backoff.min(), Backoff::DEFAULT_MIN);
        assert_eq!(policy.backoff.max(), Backoff::DEFAULT_MAX);
    }

    /// A restart policy without `reset_after` deserializes with the default [RestartPolicy::new]
    /// names, since serde fills it in apart from the constructor.
    #[cfg(feature = "serde")]
    #[test]
    fn an_omitted_reset_after_takes_the_default() {
        let config = serde_json::from_str::<ActorConfig>(
            r#"{ "supervision_strategy": { "restart": { "max_restarts": 3 } } }"#,
        )
        .expect("reset_after is optional");

        let SupervisionStrategy::Restart(policy) = config.supervision_strategy else {
            panic!("expected a restart strategy")
        };
        assert_eq!(policy.reset_after, RestartPolicy::DEFAULT_RESET_AFTER);
    }

    /// A misspelled key must be an error, not a silently applied default.
    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_rejects_unknown_fields() {
        let config =
            serde_json::from_str::<ActorConfig>(r#"{ "mailbox_capacty": { "bounded": 42 } }"#);
        assert!(config.is_err());

        let config = serde_json::from_str::<ActorConfig>(
            r#"{
                "supervision_strategy": {
                    "restart": { "max_restarts": 3, "reset_atfer": "1m" }
                }
            }"#,
        );
        assert!(config.is_err());
    }
}
