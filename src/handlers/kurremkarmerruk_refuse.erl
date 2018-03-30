-module(kurremkarmerruk_refuse).
-behavior(kurremkarmerruk_handler).
-export([execute/2,valid_opcodes/0,config_keys/0,config_init/1,config_end/1]).

valid_opcodes() -> '_'.
config_keys() -> [].


config_init(Map) -> Map.

config_end(Map) -> Map.


execute(Msg, _) ->
    {stop, dnsmsg:set_response_header(Msg, return_code, refused)}.
