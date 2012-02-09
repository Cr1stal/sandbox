-module(fridgemadness).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    PeopleList = read_input(Filename),  % list of {id, [{drinkId, drinkScore}]}, and id starts from 1,
                                        % remember to take 1 away when output
    {Men, Women} = compute_prefs(PeopleList),  % Men, Women : {id, [preferences]}
    Spouses = propose(Men, Women),

    %% output for facebook puzzle
    [begin
        {MId, WId} = Spouse,
        io:format("~B ~B~n", [MId-1, WId-1])
     end || Spouse <- Spouses].

%%===================================================================
%% private
%%===================================================================
read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),

    [Line1 | Input] = string:tokens(binary_to_list(Bin), "\r\n"),
    [P, D] = string:tokens(Line1, "\t\s"),
    NPeople = list_to_integer(P),
    NDrink = list_to_integer(D),
    Fav = lists:nthtail(NDrink, Input),
    lists:map(fun(E) ->
                Line = lists:nth(E, Fav),
                [_, Drinks] = string:tokens(Line, "\t\s"),
                DrinkIds = lists:map(fun(EE) ->
                                        list_to_integer(EE)
                                    end, string:tokens(Drinks, ",")),
                DrinkAndScores = lists:zip(DrinkIds, lists:seq(NDrink, 1, -1)),
                {E, lists:keysort(1, DrinkAndScores)}
            end, lists:seq(1, NPeople)).

compute_prefs(PeopleList) ->
    {M, W} = lists:split(length(PeopleList) div 2, PeopleList),
    Men = compute_prefs(M, W, length(PeopleList) div 2),
    Women = compute_prefs(W, M, 0),
    {Men, Women}.
compute_prefs(P1, P2, IdBase) ->
    lists:map(fun(Man) ->
                Prefs = lists:map(fun(N) ->
                                    Woman = lists:nth(N, P2),
                                    {N + IdBase, compute_pref(Man, Woman)}
                                end, lists:seq(1, length(P2))),
                {Id, _} = Man,
                {Id, Prefs}
            end, P1).

propose(M, W) ->
    %% list of {id, [woman_id sorted by pref]}
    ManSorted = lists:map(fun(E) ->
                            {Id, Women} = E,
                            WSorted = lists:keysort(2, Women),
                            {WIds, _} = lists:unzip(lists:reverse(WSorted)),
                            {Id, WIds}
                        end, M),
    %% women are stored in dict, convenient for updating
    Women = lists:foldl(fun(WW, Dict) ->
                            {Id, Prefs} = WW,
                            dict:store(Id, {0, Prefs}, Dict)
                        end, dict:new(), W),
    propose(ManSorted, Women, dict:new()).
propose([], Women, _MenEngaged) ->
    %% all done, return spouses
    Spouses = lists:map(fun(S) ->
                            {WId, {MId, _}} = S,
                            {MId, WId}
                        end, dict:to_list(Women)),
    lists:keysort(1, Spouses);
propose(MenFree, Women, MenEngaged) ->
    [ManFree | Rest] = MenFree,
    {ManEngaged, ManKickedOut, NewWomen} = propose_by(ManFree, Women, MenEngaged),
    {MId, MPrefs} = ManEngaged,
    case ManKickedOut of
        undefined -> propose(Rest, NewWomen, dict:store(MId, MPrefs, MenEngaged));
        _ -> propose(Rest ++ ManKickedOut, NewWomen, dict:store(MId, MPrefs, MenEngaged))
    end.
propose_by({MId, [PrefWId | Rest]}, Women, MenEngaged) ->
    Woman = dict:fetch(PrefWId, Women),
    {FianceId, PrefMen} = Woman,
    case FianceId =:= 0 of
        true ->
            %% not engaged yet
            NewWomen = dict:store(PrefWId, {MId, PrefMen}, Women),
            {{MId, Rest}, undefined, NewWomen};
        false ->
            PrefToProposal = lists:nth(MId, PrefMen),
            PrefToEngagement = lists:nth(FianceId, PrefMen),
            case PrefToProposal > PrefToEngagement of
                true ->
                    %% new proposal wins!
                    NewWomen2 = dict:store(PrefWId, {MId, PrefMen}, Women),
                    ManKickedOut = {FianceId, dict:fetch(FianceId, MenEngaged)},
                    {{MId, Rest}, ManKickedOut, NewWomen2};
                false ->
                    %% try next proposal
                    propose_by({MId, Rest}, Women, MenEngaged)
            end
    end.

compute_pref(P1, P2) ->
    {_, D1} = P1,
    {_, D2} = P2,
    N = length(D1),
    Prefs = lists:map(fun(E) ->
                        {_, Score1} = lists:nth(E, D1),
                        {_, Score2} = lists:nth(E, D2),
                        case Score1 =:= Score2 of
                            true -> Score1 * Score1;
                            false ->
                                case Score1 > Score2 of
                                    true -> Score1;
                                    false -> 0
                                end
                        end
                    end, lists:seq(1, N)),
    lists:foldl(fun(E, Acc) ->
                    E + Acc
                end, 0, Prefs).
