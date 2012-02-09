-module(breathalyzer).

%% API
-export([main/1]).

-define(CONCURRET_RUN, 8).

main(Args) ->
    [Filename|_] = Args,
    Dictionary = read_accepted_words("twl06.txt"),
    ConcDict = slice(Dictionary),
    Post = read_wall_post(Filename),
    Distances = [min_edit_distance(Word, ConcDict) || Word <- Post],
    TotalChanges = lists:sum(Distances),
    io:format("~B~n", [TotalChanges]).

%%===================================================================
%% private
%%===================================================================
read_accepted_words(Filename) ->
    {ok, Bin} = file:read_file(Filename),

    Words = string:tokens(binary_to_list(Bin), "[$\r$\n]+"),
    lists:map(fun(E) -> string:to_lower(E) end, Words).

read_wall_post(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    string:tokens(binary_to_list(Bin), "[$\s$\t$\r$\n]+").

slice(Dict) ->
    N = length(Dict),
    ChunkSize = case ?CONCURRET_RUN of
                    1 -> N;
                    _ -> (N div ?CONCURRET_RUN) + 1
                end,
    {ConcDict, _} = lists:foldl(fun(_E, Acc) ->
                        {L, Rest} = Acc,
                        case length(Rest) >= ChunkSize of
                            true ->
                                {Hd, Tl} = lists:split(ChunkSize, Rest),
                                {[Hd | L], Tl};
                            false ->
                                {[Rest | L], []}
                        end
                    end, {[], Dict}, lists:seq(1, ?CONCURRET_RUN)),
    conc:from_list(ConcDict).

min_edit_distance(Word, Dict) ->
    conc:mapreduce(fun(X) -> min_edit_distance(Word, X, max_int()) end,
                    fun(X, Y) -> lists:min([X, Y]) end,
                    max_int(), Dict).
min_edit_distance(_Word, [], MinDistance) ->
    MinDistance;
min_edit_distance(Word, [Head | Rest], MinDistance) ->
    Distance = edit_distance(Word, Head, MinDistance),
    min_edit_distance(Word, Rest, lists:min([Distance, MinDistance])).

edit_distance(W1, W2, Threshold) ->
    case abs(length(W1) - length(W2)) >= Threshold of
        true -> Threshold;
        false ->
            InitDistances = lists:seq(0, length(W1)),
            edit_distance(W1, W2, 1, [0], InitDistances)
    end.
edit_distance(_W1, [], _W1Index, _Distances, PrevDistances) ->
    lists:last(PrevDistances);
edit_distance(W1, [_Hd2|Tl2], W1Index, Distances, _PrevDistances) when W1Index > length(W1) ->
    edit_distance(W1, Tl2, 1, [0], lists:reverse(Distances));
edit_distance(W1, W2, W1Index, Distances, PrevDistances) ->
    Distance = compute_distance(W1, W2, W1Index, Distances, PrevDistances),
    edit_distance(W1, W2, W1Index + 1, [Distance|Distances], PrevDistances).

compute_distance(W1, [C2|_], W1Index, Distances, PrevDistances) ->
    C1 = lists:nth(W1Index, W1),
    case C1 =:= C2 of
        true ->
            lists:nth(W1Index, PrevDistances);
        false ->
            OneSubstitution = lists:nth(W1Index, PrevDistances) + 1,
            [D2|_] = Distances,
            OneInsertion =  D2 + 1,
            OneDeletion = lists:nth(W1Index + 1, PrevDistances) + 1,
            lists:min([OneSubstitution, OneInsertion, OneDeletion])
    end.

max_int() ->
    1 bsl 20.
