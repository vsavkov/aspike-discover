-module(aspike_discover).

%% API
-export([
  info/5,
  info/6,
  build/4,
  access_node/4,
  peers/4,
  nodes/4,
  namespaces/4,
  number_of_partitions/4,
  replicas/4,
  partitions/4,
  namespace_replica_node/3
]).

%% utils
-export([
  combine_namespaces_of_replicas_nodes/1,
  combine_replicas_of_namespace/1,
  gather_namespaces_of_replicas_node/2,
  gather_namespace_replicas_node/2,
  replication_factor_consistency_within_node/1,
  replication_factor_consistency_within_namespace/1,
  number_of_partitions_consistency_within_namespace/1,
  bitmap_consistency/2,
  namespace_consistency_check/1,
  replication_factor_of_replicas/1,
  number_of_partitions_of_replicas/1,
  augment_with/2
]).

%% API

%% info/6 is a working horse of aspike_discover.
%% Names parameter is a list of Info protocol commands.
%% The list of available Info protocol commands can be found at
%% https://docs.aerospike.com/server/reference/info?version=7.
%% Some of Info protocols commands:
%% - service-clear-std: to query a cluster 'seed' node for an 'access' node;
%% - node: to query Node ID;
%% - peers-clear-std: to query list of cluster nodes;
%% - namespaces: to query list of 'namespaces' available;
%% - namespace/<name-of-namespace>: to query the <name-of-namespace> parameters;
%% - partitions: to query a number of partitions;
%% - replicas: to query replicas information;
%% - best-practices;
%% - bins/[<NAMESPACE>]; - removed 7.0.0?
%% - feature-key;
%% - get-config;
%% - get-config:context=namespace;id=pi-stream;
%% - get-config:context=network;
%% - get-stats;
%% - health-outliers;
%% - health-stats;
%% - histogram:namespace=pi-stream;type=object-size;
%% - histogram:namespace=pi-stream;set=set-gateway;type=object-size;
%% - histogram:namespace=pi-stream;set=set-gateway;type=ttl;
%% - latencies:;
%% - latencies:hist={pi-stream}-read;
%% - latencies:hist={pi-stream}-write;
%% - log/0;
%% - logs;
%% - mesh;
%%
%% TODO
%% - query-show; Returns information about current/recent queries on the server node.
%%
%% - racks:
%% - roster:namespace=pi-stream
%% - service;
%% - services;
%%
%% TODO
%% - sets;
%% - sets/pi-stream;
%% - sets/pi-stream/set-gateway;
%%
%% TODO
%% - statistics; A LOT of useful info
%%
%% - status; - always returns ok, used to verify service port is open
%% - thread-traces; - needs about 10 seconds to process
%% - udf-list;
%% - etc.
info(Address, Port, User, Encrypted_password, Names) ->
  info(Address, Port, User, Encrypted_password, Names, 1000).

info(Address, Port, User, Encrypted_password, Names, Timeout) ->
  case aspike_protocol:open_session(Address, Port, User, Encrypted_password) of
    {error, Reason} ->
      {error, {Reason, info, Address, Port, User}};
    {ok, #{socket := Socket} = Session} ->
      Encoded = aspike_protocol:enc_info_request(Names),
      ok = gen_tcp:send(Socket, Encoded),
      case aspike_protocol:receive_response_info(Socket, Timeout) of
        {error, _Reason} = Err ->
          aspike_protocol:close_session(Session),
          Err;
        Response ->
          aspike_protocol:close_session(Session),
          case aspike_protocol:dec_info_response(Response) of
            need_more ->
              {error, <<"Not enough data to decode response">>};
            {error, _Reason} = Err -> Err;
            {ok, _Decoded, _Rest} = Ret ->
              Ret
          end
      end
  end.

build(Address, Port, User, Encrypted_password) ->
  Name = <<"build">>,
  case info(Address, Port, User, Encrypted_password, [Name]) of
    {error, Reason} ->
      {error, {Reason, build, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name, Decoded) of
        undefined -> {error, no_build_information};
        Value -> Value
      end
  end.

access_node(Address, Port, User, Encrypted_password) ->
  Name = <<"service-clear-std">>,
  case info(Address, Port, User, Encrypted_password, [Name]) of
    {error, Reason} ->
      {error, {Reason, access_node, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name, Decoded) of
        undefined -> {error, no_node_access_address};
        Value ->
          parse_string_integer(Value)
      end
  end.

peers(Address, Port, User, Encrypted_password) ->
  Node_name_request = <<"node">>, Peers_request = <<"peers-clear-std">>,
  case info(Address, Port, User, Encrypted_password, [Node_name_request, Peers_request]) of
    {error, Reason} ->
      {error, {Reason, peers, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      Node_name_response = proplists:get_value(Node_name_request, Decoded),
      Peers_response = proplists:get_value(Peers_request, Decoded),
      case aspike_util:to_term(binary_to_list(Peers_response)) of
        [Generation_str, Default_port_str, Peers_term] ->
          Generation = to_integer_or_undefined(Generation_str),
          Default_port = to_integer_or_undefined(Default_port_str),
          Peers = [to_peer(X) || X <- Peers_term],
          Peers_augmented =
            [P#{port => Default_port} || P <- Peers],
          {ok, #{
            name => Node_name_response,
            address => if is_binary(Address) -> Address;
                         true -> list_to_binary(Address) end,
            port => Port,
            generation => Generation,
            peers => Peers_augmented
          }};
        X ->
          {error, {wrong_peers_response_format, X}}
      end
  end.

nodes(Address, Port, User, Encrypted_password) ->
  case peers(Address, Port, User, Encrypted_password) of
    {error, _Reason} = Err -> Err;
    {ok, #{name := Node_name, address := Node_address, port := Node_port,
      peers := Peers}} ->
      lists:foldl(fun (#{name := N, address := A, port := P}, Acc) ->
        [{N, A, P}|Acc] end, [{Node_name, Node_address, Node_port}], Peers)
  end.

namespaces(Address, Port, User, Encrypted_password) ->
  Name = <<"namespaces">>,
  case info(Address, Port, User, Encrypted_password, [Name]) of
    {error, Reason} ->
      {error, {Reason, namespaces, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name, Decoded) of
        undefined -> {error, no_namespaces};
        Value ->
          binary:split(Value, [<<";">>], [global])
      end
  end.

number_of_partitions(Address, Port, User, Encrypted_password) ->
  Name = <<"partitions">>,
  case info(Address, Port, User, Encrypted_password, [Name]) of
    {error, Reason} ->
      {error, {Reason, number_of_partitions, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name, Decoded) of
        undefined -> {error, no_number_of_partitions};
        Value ->
          case aspike_protocol:info_number_of_partitions(Value) of
            {ok, V} -> V;
            {error, Reason} -> {error, {Reason, no_number_of_partitions}}
          end
      end
  end.

replicas(Address, Port, User, Encrypted_password) ->
  Name = <<"replicas">>,
  case info(Address, Port, User, Encrypted_password, [Name]) of
    {error, Reason} ->
      {error, {Reason, replicas, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name, Decoded) of
        undefined -> {error, no_replicas};
        Value ->
          aspike_protocol:info_replicas(Value)
      end
  end.

partitions(Address, Port, User, Encrypted_password) ->
  Name_partitions = <<"partitions">>, Name_replicas = <<"replicas">>,
  case info(Address, Port, User, Encrypted_password, [Name_partitions, Name_replicas]) of
    {error, Reason} ->
      {error, {Reason, partitions, Address, Port, User}};
    {ok, Decoded, _Rest} ->
      case proplists:get_value(Name_partitions, Decoded) of
        undefined -> {error, number_of_partitions_not_provoded};
        Partitions ->
          case aspike_protocol:info_number_of_partitions(Partitions) of
            {error, Reason} -> {error, {Reason, number_of_partitions_not_provoded}};
            {ok, N_partitions} ->
              case proplists:get_value(Name_replicas, Decoded) of
                undefined -> {error, replicas_not_provided};
                Replicas ->
                  Decoded_replicas = aspike_protocol:info_replicas(Replicas),
                  [{Namespace, N_regime, N_replication_factor, N_partitions,
%%                    [aspike_protocol:info_bitmap_to_map(
%%                      aspike_protocol:info_dec_partitions_bitmap(N_partitions, X), 0, #{})
                    [aspike_protocol:info_bitmap_to_set(
                      aspike_protocol:info_dec_partitions_bitmap(N_partitions, X),
                      0, sets:new([{version, 2}]))
                      || X <- Based64_bitmaps]}
                    || {Namespace, {N_regime, N_replication_factor, Based64_bitmaps}} <- Decoded_replicas]
              end
          end
      end
  end.

%% Output:
%% #{
%%  Ns1 => {
%%      Replica1 = #{
%%        0 => {Node_name1, Node_address1, Node_port1},
%%        1 => {Node_name2, Node_address2, Node_port2},
%%        ...
%%        15 => {Node_name2, Node_address2, Node_port2}
%%      },
%%      Replica2 = #{
%%        ...
%%      }
%%    },
%%  Ns2 => {
%%      ...
%%    }
%% }
namespace_replica_node(Nodes, User, Encrypted_password) ->
  Partitions = [{Node, partitions(Address, Port, User, Encrypted_password)} || {_Name, Address, Port} = Node <- Nodes],
  List_of_namespaces_of_replicas_node = [gather_namespaces_of_replicas_node(Node, Namespaces)
    || {{_Node_name, _Address, _Port} = Node, Namespaces} <- Partitions],
  Namespaces_map = combine_namespaces_of_replicas_nodes(List_of_namespaces_of_replicas_node),

  Namespaces_list = maps:to_list(Namespaces_map),
  Namespaces_replicas = [{Namespace, combine_replicas_of_namespace(List_of_replicas)}
    || {Namespace, List_of_replicas} <- Namespaces_list],
  Namespace_to_replicas = maps:from_list(Namespaces_replicas),
  Namespace_to_replicas.

%% utils
combine_namespaces_of_replicas_nodes(List_of_namespaces_of_replicas_node) ->
  lists:foldl(fun (Namespaces_of_replicas_node, Acc) ->
    maps:merge_with(fun (_Key, V1, V2) -> lists:flatten([V2 | [V1]]) end,
      Acc, Namespaces_of_replicas_node)
              end, #{}, List_of_namespaces_of_replicas_node).

replication_factor_of_replicas([{_N_regime, N_replication_factor, _N_partitions, _Replicas} | _]) ->
  N_replication_factor;
replication_factor_of_replicas(_) -> 0.

number_of_partitions_of_replicas([{_N_regime, _N_replication_factor, N_partitions, _Replicas} | _]) ->
  N_partitions;
number_of_partitions_of_replicas(_) -> 0.

combine_replicas_of_namespace(Replicas_of_namespace) ->
  case replication_factor_of_replicas(Replicas_of_namespace) of
    0 -> {0, 0, []};
    N_replication_factor ->
      Combined_replicas_acc = lists:duplicate(N_replication_factor, #{}),
      Combined_replicas = lists:foldl(
        fun ({_N_regime, _N_replication_factor, _N_partitions, Replicas}, Acc) ->
          To_combine = lists:zip(Acc, Replicas),
          Combined = [
            maps:merge_with(fun (_Key, V1, V2) -> lists:flatten([V2 | [V1]]) end, M1, M2)
            || {M1, M2} <- To_combine],
          Combined
        end, Combined_replicas_acc, Replicas_of_namespace),
      N_partitions = number_of_partitions_of_replicas(Replicas_of_namespace),
      {N_replication_factor, N_partitions, Combined_replicas}
  end.

gather_namespaces_of_replicas_node({_Node_name, _Address, _Port} = Node, Namespaces) ->
  maps:from_list([gather_namespace_replicas_node(Node, Namespace)
    || Namespace <- Namespaces]).

gather_namespace_replicas_node({_Node_name, _Address, _Port} = Node,
    {Namespace_name, N_regime, N_replication_factor, N_partitions, Replicas} = _Namespace) ->
  {Namespace_name, {N_regime, N_replication_factor, N_partitions,
    [augment_with(Replica, Node) || Replica <- Replicas]}}.

namespace_consistency_check(Namespace_as_list_of_replicas) when is_list(Namespace_as_list_of_replicas) ->
  case replication_factor_consistency_within_node(Namespace_as_list_of_replicas) of
    {error, _Reason} = Err -> Err;
    true ->
      case replication_factor_consistency_within_namespace(Namespace_as_list_of_replicas) of
        {error, _Reason} = Err -> Err;
        true ->
          case number_of_partitions_consistency_within_namespace(Namespace_as_list_of_replicas) of
            {error, _Reason} = Err -> Err;
            true -> true
          end
      end
  end.

%% replication_factor consistency withing a node
replication_factor_consistency_within_node(Namespace_as_list_of_replicas) when is_list(Namespace_as_list_of_replicas) ->
  case Not_consistent = [X || {_N_regime, _N_replication_factor, _N_partitions, _Replicas} =
    X <- Namespace_as_list_of_replicas, not replication_factor_consistency_within_node(X)] of
    [] -> true;
    _ -> {error, {replication_factor_not_consistent_withing_node, Not_consistent}}
  end;
replication_factor_consistency_within_node({_N_regime, N_replication_factor, _N_partitions, Replicas}) ->
  is_list(Replicas) andalso length(Replicas) =:= N_replication_factor.

%% replication_factor consistency withing a namespace
replication_factor_consistency_within_namespace(Namespace_as_list_of_replicas) when is_list(Namespace_as_list_of_replicas) ->
  case Uniq = lists:uniq(
    fun ({_N_regime, N_replication_factor, _N_partitions, _Replicas}) -> N_replication_factor end,
    Namespace_as_list_of_replicas) of
    [_] -> true;
    _ -> {error, {replication_factor_not_consistent_within_namespace, Uniq}}
  end.

%% number of partitions consistency withing a namespace
number_of_partitions_consistency_within_namespace(Namespace_as_list_of_replicas) when is_list(Namespace_as_list_of_replicas) ->
  case Uniq = lists:uniq(
    fun ({_N_regime, _N_replication_factor, N_partitions, _Replicas}) -> N_partitions end,
    Namespace_as_list_of_replicas) of
    [_] -> true;
    _ -> {error, {number_of_partitions_not_consistent_within_namespace, Uniq}}
  end.

%% bitmap consistency
bitmap_consistency(N_required_elements, #{} = Bitmap) ->
  case maps:size(Bitmap) of
    N_required_elements ->
      L = maps:to_list(Bitmap),
      {Partition_numbers, _} = lists:unzip(L),
      Partition_numbers_ordered = lists:sort(Partition_numbers),
      case Partition_numbers_ordered =:= lists:seq(0, N_required_elements-1) of
        false ->
          {error, {bitmap_keys_are_not_from, 0, to, N_required_elements-1, Partition_numbers_ordered}};
        true ->
          More_than_one_element_per_key = [E || E <- L, is_list(E)],
          case [E || E <- L, is_list(E)] of
            [] -> true;
            More_than_one_element_per_key ->
              {error, {more_than_one_element, More_than_one_element_per_key}}
          end
      end;
    Size ->
      {error, {number_of_required_elements, N_required_elements, actual_number_of_elements, Size}}
  end;
bitmap_consistency(_, _) ->
  {error, not_bitmap}.

augment_with(Set, X) ->
  case sets:is_set(Set) of
    true ->
      maps:from_list([{Elem, X} || Elem <- sets:to_list(Set)]);
    _ -> #{}
  end.

parse_string_integer(Data) ->
  case binary:split(Data, [<<":">>]) of
    [S, S2] ->
      case string_to_integer(binary_to_list(S2)) of
        {ok, I} -> {ok, {S, I}};
        {error, _} -> {error, {wrong_string_integer_format, Data}}
      end;
    _ -> {error, {wrong_string_integer_format, Data}}
  end.

string_to_integer(S) ->
  try list_to_integer(S) of
    V -> {ok, V}
  catch error:badarg ->
    {error, not_an_integer, S}
  end.

to_integer_or_undefined(S) ->
  case string_to_integer(S) of
    {ok, V} -> V;
    _ -> undefined
  end.

to_peer([Name, Tls_name, [H|_T] = Hosts]) ->
  #{
    name => list_to_binary(Name),
    tls_name => list_to_binary(Tls_name),
    address => list_to_binary(H),
    hosts => [list_to_binary(X) || X <- Hosts]
  };
to_peer(X) ->
  {error, {wrong_peer_format, X}}.
