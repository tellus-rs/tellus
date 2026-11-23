### Renamed error variants

Four error variants are renamed to match the rest of the API, without any change in behavior:

- `InvalidPhiAccrual::MaxSampleSize` is now `InvalidPhiAccrual::MaxSampleSizeTooSmall`.
- `InvalidPhiAccrual::MinStdDeviation` is now `InvalidPhiAccrual::ZeroMinStdDeviation`.
- `InvalidPhiAccrual::WarmupDeadline` is now `InvalidPhiAccrual::ZeroWarmupDeadline`.
- `LeaveError::NotAMember` is now `LeaveError::Unformed`, after `Lifecycle::Unformed`, the state
  it is returned for.

Code matching any of them has to use the new names.
