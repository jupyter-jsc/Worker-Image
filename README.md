# Worker

Backend service for [JupyterHub](https://github.com/jupyter-jsc/Hub-Image)

## Mounts
The following paths have to be mounted:

```
/etc/j4j/j4j_mount/j4j_common
/etc/j4j/j4j_mount/j4j_token
/etc/j4j/j4j_mount/jobs
/etc/j4j/j4j_mount/j4j_worker
```

To understand what's in each of these directories, have a look at our [Configuration](https://github.com/jupyter-jsc/Configuration) repository.
