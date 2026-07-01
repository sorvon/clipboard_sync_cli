# clipboard_sync_cli

Since the OSC52(**read**) is not support by many terminals, I wrote this to sync
clipboard data when using nvim remotely.

## Usage

### nvim

#### On local machine

Linux

```bash
clipboard_sync_cli -s [ip:port] -d [DISPLAY/WAYLAND_DISPLAY]
```

Windows

```bash
clipboard_sync_cli -s [ip:port] -d
```

-s means lauch a http/grpc server, default is `0.0.0.0:11457`
-d means watch the local clipboard change

#### On nvim config

```lua
vim.g.clipboard = {
  name = "clipboard_sync_cli",
  copy = {
    ["+"] = { "clipboard_sync_cli", "set", "-s", "127.0.0.1:11457"},
    ["*"] = { "clipboard_sync_cli", "set", "-s", "127.0.0.1:11457"},
  },
  paste = {
    ["+"] = { "clipboard_sync_cli", "get", "-s", "127.0.0.1:11457"},
    ["*"] = { "clipboard_sync_cli", "get", "-s", "127.0.0.1:11457"},
  },
  cache_enabled = 1,
}
```

Or you can set the CLIPBOARD_SYNC_CLI_SERVER environment, and you just need

```lua
vim.g.clipboard = {
  name = "clipboard_sync_cli",
  copy = {
    ["+"] = { "clipboard_sync_cli", "set"},
    ["*"] = { "clipboard_sync_cli", "set"},
  },
  paste = {
    ["+"] = { "clipboard_sync_cli", "get"},
    ["*"] = { "clipboard_sync_cli", "get"},
  },
  cache_enabled = 1,
}
```

### Normal sync

#### On one machine

```bash
clipboard_sync_cli -s [ip:port] -d
```

#### On others

```bash
clipboard_sync_cli -c [ip:port] -d
```
