-module(swarm).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    Planets = read_input(Filename),
    Minings = lists:map(fun(P) -> attack_and_mine(P) end, Planets),
    print_result(Planets, Minings).

%%==========================================================
%% private
%%==========================================================
read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Input = string:tokens(binary_to_list(Bin), "\r\n"),
    P = list_to_integer(hd(Input)),
    Planets = read_input(tl(Input), P, []),
    Planets.
read_input(_Input, 0, Planets) ->
    lists:reverse(Planets);
read_input(Input, P, Planets) ->
    LineOne = hd(Input),
    [T, Z] = string:tokens(LineOne, "\t\s"),
    {NewInput, Bases} = read_terran_bases(tl(Input), 0, list_to_integer(T), []),
    read_input(NewInput, P-1, [{list_to_integer(Z), Bases}|Planets]).
read_terran_bases(Input, Index, T, Bases) when Index =:= T ->
    {Input, lists:reverse(Bases)};
read_terran_bases(Input, Index, T, Bases) ->
    Line = hd(Input),
    [S, M] = string:tokens(Line, "\t\s"),
    read_terran_bases(tl(Input), Index + 1, T, [{Index, list_to_integer(S), list_to_integer(M)}|Bases]).

attack_and_mine(P) ->
    {T, Bases} = P,
    TT = T div 3,
    Attackables = lists:filter(fun(B) -> {_, S, _} = B, S =< TT end, Bases),
    M0 = lists:duplicate(TT+1, {0, 0, []}),
    M = attack_and_mine(TT, Attackables, M0),
    lists:last(M).
attack_and_mine(_TT, [], M) ->
    M;
attack_and_mine(TT, [Base|Rest], MUp) ->
    M = attack_and_mine_one_base(1, TT, Base, MUp, [{0, 0, []}]),
    attack_and_mine(TT, Rest, M).
attack_and_mine_one_base(Index, TT, _, _, M) when Index > (TT+1) ->
    M;
attack_and_mine_one_base(Index, TT, Base, MUp, M) ->
    {BaseIndex, S, Mine} = Base,
    {S1, V1, SS1} = lists:nth(Index, MUp),
    NewM = case S > (Index-1) of
            true ->
                M ++ [{S1, V1, SS1}];
            false ->
                {S2, V2, SS2} = lists:nth(Index - S, MUp),
                case V1 > (V2 + Mine) of
                    true ->
                        M ++ [{S1, V1, SS1}];
                    false ->
                        M ++ [{S2 + S, V2 + Mine, SS2 ++ [BaseIndex]}]
                end
            end,
    attack_and_mine_one_base(Index+1, TT, Base, MUp, NewM).

print_result(Planets, Minings) ->
    [begin
        {S, M, Bases} = lists:nth(Index, Minings),
        io:format("~B ~B~n", [S*3, M]),
        {_, AllBases} = lists:nth(Index, Planets),
        [begin
            {_, T, _} = lists:nth(BaseIndex + 1, AllBases),
            io:format("~B ~B", [BaseIndex, T*3]),
            case BaseIndex =:= length(Bases) of
                false -> io:format(" ");
                true -> ignore
            end
         end|| BaseIndex <- Bases],
         [io:format("~n") || length(Bases) > 0]
     end || Index <- lists:seq(1, length(Planets))].
