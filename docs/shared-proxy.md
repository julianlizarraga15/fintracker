# Maintain the shared Fintracker and watering proxy

Fintracker and watering-system share the EC2 host and the `fintracker-nginx` container. The tracked `docker-compose.proxy.yml` declares that existing proxy separately from the Fintracker web services in `docker-compose.isolated.yml`.

The proxy keeps its existing image, container name, ports, Nginx configuration mount, and read-only `/etc/letsencrypt` mount. It joins the existing external networks `fintracker_default` and `watering-proxy`. The proxy manifest does not create or own either network. `nginx.conf` sends Fintracker requests to `backend` and `frontend`, and watering requests to `watering-backend`.

## Validate the explicit proxy manifest

From the reviewed checkout, validate configuration without starting or recreating services:

```bash
docker-compose --env-file /dev/null -p fintracker \
  -f docker-compose.proxy.yml config --quiet
```

The explicit file and interpolation environment prevent default Compose discovery from loading a legacy environment file. This command does not establish that the live networks, containers, or certificates match the manifest.

Normal web releases use `scripts/deploy_isolated_web.sh`. They update only the selected Fintracker services and validate and reload the existing shared proxy. Applying or recreating the proxy is a separate maintenance operation that must account for both applications. Never use Compose `down` or orphan removal on this shared deployment.

## Retire the old file on EC2

Removing the default Compose file from Git does not remove it from the host. The release helper fetches an exact commit and reads deployment files from Git without resetting the host checkout. Preserve unrelated host changes.

Before retiring the host's old `docker-compose.yml`:

1. Inventory the running Fintracker and watering containers, their image IDs, the proxy's bind mount paths, and membership and aliases on both networks. Inspect only those metadata fields. Do not dump container environments or read any `.env` contents.
2. Preserve the current proxy image and reviewed configuration, plus the previous working credential-free Fintracker images needed for rollback. Preserve persistent data and certificate directories.
3. Check host scripts, timers, and external deployment callers for reliance on default Compose discovery or the retired `scripts/local_build_and_up.sh` helper.
4. Place the reviewed `docker-compose.proxy.yml` on the host and validate it with the explicit command above. Confirm that it matches the inventory before retiring only the identified legacy files.
5. Confirm that both public applications and the valuation timer remain healthy after file retirement. Retiring the file does not require proxy or watering recreation.

Do not use `git reset`, `git clean`, broad Docker pruning, network deletion, or volume deletion to perform this cleanup. The isolated web release continues to use the existing networks and data mount.
