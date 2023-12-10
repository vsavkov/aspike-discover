-module(aspike_discover).


-export([
  access_node/4
]).

%% API
access_node(Address, Port, User, Encrypted_password) ->
  case aspike_protocol:open_session(Address, Port, User, Encrypted_password) of
    {error, _Reason} = Err -> Err;
    {ok, #{socket := Socket} = Session} ->
      Encoded = aspike_protocol:enc_names_request(["service-clear-std"]),
      ok = gen_tcp:send(Socket, Encoded),
      Response = aspike_protocol:receive_response_info(Socket, 1000),
      aspike_protocol:close_session(Session),
      Decoded = aspike_protocol:dec_names_response(Response),
      Decoded
  end.
