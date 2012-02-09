-module(gattaca).

%% API
-export([main/1]).


%%===================================================================
%% API
%%===================================================================
main(Args) ->
    [Filename | _] = Args,
    Intervals = read_and_sort_intervals(Filename),
    BestScore = compute_max_score(Intervals),
    io:format("~B~n", [BestScore]).

%%===================================================================
%% private
%%===================================================================
read_and_sort_intervals(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Input = string:tokens(binary_to_list(Bin), "\r\n"),

    %% skip dns strand data, which is irrelevant
    N = list_to_integer(hd(Input)),
    NewInput = read_and_discard(tl(Input), N),

    %% read up all intervals
    I = list_to_integer(hd(NewInput)),
    Intervals = read_intervals(tl(NewInput), I),

    %% sort interval by its stop
    lists:keysort(2, Intervals).

read_and_discard([_Hd|Tl], N) when N > 0 ->
    read_and_discard(Tl, N - 80);
read_and_discard(Input, _) ->
    Input.

read_intervals(Input, N) ->
    read_intervals(Input, N, []).
read_intervals(_Input, 0, Acc) ->
    Acc;
read_intervals(Input, N, Acc) ->
    Line = hd(Input),
    [Start, Stop, Score|_] = string:tokens(Line, "\s\t\n"),
    read_intervals(tl(Input), N - 1, [{list_to_integer(Start),
                                       list_to_integer(Stop),
                                       list_to_integer(Score)} | Acc]).

compute_max_score(Intervals) ->
    compute_max_score(Intervals, [], 1).
compute_max_score(Intervals, Scores, N) when N > length(Intervals) ->
    lists:nth(length(Intervals), Scores);
compute_max_score(Intervals, Scores, N) ->
    {_, _, Score} = lists:nth(N, Intervals),
    AncestorIndex = find_ancestor(Intervals, N),
    CurrentMaxIncludeMe =
        case AncestorIndex of
            0 ->
                Score;
            _ ->
                Score + lists:nth(AncestorIndex, Scores)
        end,
    CurrentMaxExcludeMe = 
        case N of
            1 ->
                Score;
            _ ->
                lists:nth(N - 1, Scores)
        end,
    CurrentMax = lists:max([CurrentMaxIncludeMe, CurrentMaxExcludeMe]),
    NewScores = lists:append(Scores, [CurrentMax]),
    compute_max_score(Intervals, NewScores, N + 1).

%% 'ancestor' of an interval is the interval prior to but not overlap with it
%% can be 0 if given interval has no 'ancestor'
find_ancestor(Intervals, N) ->
    find_ancestor(Intervals, N, N - 1).
find_ancestor(_, _, 0) ->
    0;
find_ancestor(Intervals, N, M) ->
    {Start, _, _} = lists:nth(N, Intervals),
    {_, Stop, _} = lists:nth(M, Intervals),
    case Start >= Stop of
        true -> M;
        false -> find_ancestor(Intervals, N, M - 1)
    end.
