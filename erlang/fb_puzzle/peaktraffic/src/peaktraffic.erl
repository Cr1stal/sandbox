-module(peaktraffic).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    UsersGraph = read_input(Filename),  % stored in a gb_tree, whose key is user, value is [neighbours]
    bron_kerbosch(UsersGraph),
    print_cliques().

%%===================================================================
%% private
%%===================================================================
bron_kerbosch(UsersGraph) ->
    P = gb_trees:keys(UsersGraph),
    ets:new(tab_name(), [ordered_set, named_table]),
    bron_kerbosch([], P, [], UsersGraph).
bron_kerbosch(R, [], [], _) ->
    [H|Rest] = lists:sort(R),
    Clique = H ++ lists:foldl(fun(E, Acc) -> Acc ++ ", " ++ E end, "", Rest),
    ets:insert(tab_name(), {Clique});
bron_kerbosch(R, P, X, UsersGraph) ->
    Pivot = choose_pivot(lists:append(P, X), UsersGraph),
    PivotNeighbours = gb_trees:get(Pivot, UsersGraph),
    NonPivotNeighbours = lists:filter(fun(E) -> lists:member(E, PivotNeighbours) =:= false end, P),
    lists:foldl(fun(V, Acc) ->
        {PP, XX} = Acc,
        VNeighbours = gb_trees:get(V, UsersGraph),
        NewP = lists:filter(fun(E) -> lists:member(E, VNeighbours) end, PP),
        NewX = lists:filter(fun(E) -> lists:member(E, VNeighbours) end, XX),
        bron_kerbosch([V|R], NewP, NewX, UsersGraph),
        {lists:delete(V, PP), [V|XX]}
     end, {P, X}, NonPivotNeighbours).

choose_pivot([Pivot|PX], UsersGraph) ->
    choose_pivot(Pivot, PX, UsersGraph).
choose_pivot(Pivot, [], _) ->
    Pivot;
choose_pivot(Pivot, [NewPivot|PX], UsersGraph) ->
    N = gb_trees:get(Pivot, UsersGraph),
    NN = gb_trees:get(NewPivot, UsersGraph),
    case length(NN) > length(N) of
        true -> choose_pivot(NewPivot, PX, UsersGraph);
        false -> choose_pivot(Pivot, PX, UsersGraph)
    end.

read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Graph1 = build_user_graph(string:tokens(binary_to_list(Bin), "\r\n")),

    %% find all neighbours
    Graph2 = gb_trees_map(fun(K, V) ->
                            find_neighbours(K, [], V, Graph1)
                        end, Graph1),
    Graph2.

build_user_graph(Input) ->
    build_user_graph(Input, gb_trees:empty()).
build_user_graph([], Graph) ->
    Graph;
build_user_graph([Line|Rest], Graph) ->
    [_, _, _, _, _, _, User1, User2] = string:tokens(Line, "\t\s"),
    NG = case gb_trees:is_defined(User1, Graph) of
            true ->
                SendToes = gb_trees:get(User1, Graph),
                gb_trees:update(User1, [User2|SendToes], Graph);
            false ->
                gb_trees:insert(User1, [User2], Graph)
        end,
    build_user_graph(Rest, NG).

find_neighbours(_, Neighbours, [], _) ->
    Neighbours;
find_neighbours(U1, Neighbours, [U2|Rest], Users) ->
    L = gb_trees:get(U2, Users),
    case lists:member(U1, L) of
        true ->
            find_neighbours(U1, [U2|Neighbours], Rest, Users);
        false ->
            find_neighbours(U1, Neighbours, Rest, Users)
    end.

print_cliques() ->
    ets:foldl(fun({Clique}, Acc) ->
                io:format("~s~n", [Clique]),
                Acc
            end, [], tab_name()).

tab_name() ->
    peaktraffic.

%% erlang 5.5.5 (R11B05) doesn't have gb_trees:map/2, yet
gb_trees_map(F, {Size, Tree}) when is_function(F, 2) ->
    {Size, map_1(F, Tree)}.

map_1(_, nil) -> nil;
map_1(F, {K, V, Smaller, Larger}) ->
    {K, F(K, V), map_1(F, Smaller), map_1(F, Larger)}.
