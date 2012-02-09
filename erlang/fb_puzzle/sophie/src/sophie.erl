-module(sophie).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    {Locations, InitialPaths} = read_input(Filename),
    Paths = floyd_warshall(Locations, InitialPaths),
    search_tour(Locations, Paths),
    print_result().

%%==========================================================
%% private
%%==========================================================
floyd_warshall(Locations, InitialPaths) ->
    N = dict:size(Locations),
    lists:foldl(fun(K, AccK) ->
        lists:foldl(fun(I, AccI) ->
            lists:foldl(fun(J, AccJ) ->
                IJ = get_matrix_cell(I, J, N, AccJ),
                IK = get_matrix_cell(I, K, N, AccJ),
                KJ = get_matrix_cell(K, J, N, AccJ),
                Min = lists:min([IJ, IK + KJ]),
                set_matrix_cell(I, J, N, Min, AccJ)
            end, AccI, lists:seq(0, N - 1))
        end, AccK, lists:seq(0, N - 1))
    end, InitialPaths, lists:seq(0, N - 1)).

search_tour(Locs, Paths) ->
    ets:new(tab_name(), [set, named_table]),
    ets:insert(tab_name(), {min_time, max_int()}),
    LocOdds = lists:sort(fun(L1, L2) ->
                            {Index1, _} = L1,
                            {Index2, _} = L2,
                            Index1 =< Index2
                        end, [Loc || {_, Loc} <- dict:to_list(Locs)]),
    [Loc|LocRest] = LocOdds,
    {_, IntiOdds} = Loc,
    search_tour(Loc, LocRest, length(LocOdds), Paths, 0, 1.0 - IntiOdds, 0.0).
search_tour(_Loc, [], _, _Paths, _Seconds, _OddsRemain, ExpectedTime) ->
    %% visited all locations, must have found sophie
    [{min_time, MinTime}] = ets:lookup(tab_name(), min_time),
    ets:insert(tab_name(), {min_time, lists:min([ExpectedTime, MinTime])});
search_tour(Loc, LocRemain, NLoc, Paths, Seconds, OddsRemain, ExpectedTime) ->
    [{_, MinTime}] = ets:lookup(tab_name(), min_time),
    case (Seconds * OddsRemain + ExpectedTime) < MinTime of
        true ->
            LocSorted = sort_locs_by_distance(Loc, LocRemain, NLoc, Paths),
            [begin
                {LocIndex, _} = Loc,
                {NextLocIndex, NextLocOdds} = NextLoc,
                NextPathSeconds = get_matrix_cell(LocIndex, NextLocIndex, NLoc, Paths),
                NewSeconds = Seconds + NextPathSeconds,
                NewOdds = OddsRemain - NextLocOdds,
                NewExpected = ExpectedTime + NewSeconds * NextLocOdds,
                search_tour(NextLoc, lists:delete(NextLoc, LocSorted), NLoc, Paths, NewSeconds, NewOdds, NewExpected)
            end || NextLoc <- LocSorted];
        false ->
            %% pruning
            ignore
    end.

sort_locs_by_distance(Loc, Locs, Size, Paths) ->
    lists:sort(fun(L1, L2) ->
                    {Index, _} = Loc,
                    {Index1, _} = L1,
                    {Index2, _} = L2,
                    get_matrix_cell(Index, Index1, Size, Paths) =< get_matrix_cell(Index, Index2, Size, Paths)
                end, Locs).

read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Input = string:tokens(binary_to_list(Bin), "\r\n"),
    NLocation = list_to_integer(hd(Input)),
    {NewInput, Locations} = read_location(tl(Input), 0, NLocation, dict:new()),
    NPath = list_to_integer(hd(NewInput)),
    Paths = read_path(tl(NewInput), NPath, Locations, init_matrix(NLocation, NLocation)),
    {Locations, Paths}.
read_location(Input, Index, NLocation, Locations) when Index >= NLocation ->
    {Input, Locations};
read_location(Input, Index, NLocation, Locations) ->
    [LocName, Probability] = string:tokens(hd(Input), "\t\s"),
    Loc = {Index, list_to_float("0" ++ Probability)},
    read_location(tl(Input), Index+1, NLocation, dict:store(LocName, Loc, Locations)).
read_path(_, 0, _, Paths) ->
    Paths;
read_path(Input, N, Locations, Paths) ->
    [Loc1, Loc2, Seconds] = string:tokens(hd(Input), "\t\s"),
    {Index1, _} = dict:fetch(Loc1, Locations),
    {Index2, _} = dict:fetch(Loc2, Locations),
    Sec = list_to_integer(Seconds),
    NLoc = dict:size(Locations),
    NewPaths = set_matrix_cell(Index1, Index2, NLoc, Sec, set_matrix_cell(Index2, Index1, NLoc, Sec, Paths)),
    read_path(tl(Input), N-1, Locations, NewPaths).

print_result() ->
    [{min_time, MinTime}] = ets:lookup(tab_name(), min_time),
    case MinTime == max_int() of
        false -> io:format("~.2f~n", [MinTime]);
        true -> io:format("~.2f~n", [-1.00])
    end.

init_matrix(X, Y) ->
    Matrix = lists:foldl(fun(E, Acc) ->
                            dict:store(E, max_int(), Acc)
                        end, dict:new(), lists:seq(0, X * Y - 1)),
    Matrix.
set_matrix_cell(Col, Row, Size, Value, Matrix) ->
    dict:store(Col+Row*Size, Value, Matrix).
get_matrix_cell(Col, Row, Size, Matrix) ->
    dict:fetch(Col+Row*Size, Matrix).
max_int() ->
    %% a little arbitrary
    2 bsl 20.
tab_name() ->
    sophie.

