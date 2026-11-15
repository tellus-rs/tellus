### Bootstrap config fields, the formation view and the codec error constructors are renamed

These names in the cluster API now say what they are:

| Old | New |
| --- | --- |
| `BootstrapConfig::min_peers` | `BootstrapConfig::min_nodes` |
| `BootstrapConfig::DEFAULT_MIN_PEERS` | `BootstrapConfig::DEFAULT_MIN_NODES` |
| `BootstrapConfig::formation` | `BootstrapConfig::formation_provider` |
| `cluster::formation::Formation` | `cluster::formation::FormationView` |
| `CodecError::encoding` | `CodecError::make_encoding` |
| `CodecError::decoding` | `CodecError::make_decoding` |

The YAML keys of `BootstrapConfig` change with its fields, and since unknown keys are refused, a
configuration file with the old keys no longer loads. Rename them:

```yaml
bootstrap:
  min_peers: 5
  formation: majority
```

becomes

```yaml
bootstrap:
  min_nodes: 5
  formation_provider: majority
```

A custom `FormationProvider` takes `&FormationView` in `observe` and `form`, and a custom `Schema`
implementation calls `CodecError::make_encoding` and `CodecError::make_decoding`.
