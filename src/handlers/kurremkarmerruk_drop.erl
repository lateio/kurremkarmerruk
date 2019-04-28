-module(kurremkarmerruk_drop).
-behavior(kurremkarmerruk_handler).
-export([
    execute/2,
    valid_opcodes/0,
    config_keys/0,
    config_init/1,
    config_end/1,
    spawn_handler_proc/0
]).

spawn_handler_proc() -> false.
valid_opcodes() -> '_'.
config_keys() -> [].


config_init(Map) -> Map.

config_end(Map) -> Map.


execute(_, _) -> drop.
