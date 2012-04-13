-module(usrbincrash).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    {WeightToLoss, Cargoes} = read_input(Filename),
    Cost = compute_min_loss(WeightToLoss, Cargoes),
    io:format("~B~n", [Cost]).

%%==========================================================
%% private
%%==========================================================
read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    [W | C] = string:tokens(binary_to_list(Bin), "\r\n"),
    CC = lists:map(fun(E) ->
                        [_, Weight, Cost] = string:tokens(E, "\s\t"),
                        {list_to_integer(Weight), list_to_integer(Cost)}
                    end, C),
    Weight = list_to_integer(W),
    %% reduce by gcd of all weight can help
    GCD = lists:foldl(fun(E, Acc) ->
                        {WW, _} = E,
                        gcd(WW, Acc)
                    end, Weight, CC),
    Cargoes = lists:map(fun(E) ->
                            {WWW, Cost} = E,
                            {WWW div GCD, Cost}
                        end, CC),
    {Weight div GCD, Cargoes}.

compute_min_loss(_, []) ->
    0;
compute_min_loss(Target, Cargoes) ->
    {_, Costs} = lists:unzip(Cargoes),
    MaxCost = lists:max(Costs) + 1,
    InitSeq = lists:map(fun(E) ->
                            {E, {E, MaxCost*E}}
                        end, lists:seq(1, Target)),
    compute_min_loss(Cargoes, InitSeq, []).
compute_min_loss([], Seq, _) ->
    [{_, {_, Cost}}|_] = lists:reverse(Seq),
    Cost;
compute_min_loss([_Cargo|Rest], [], Seq) ->
    compute_min_loss(Rest, Seq, []);
compute_min_loss(Cargoes, [PrevChoice|RestPrevSeq], []) ->
    [Cargo|_] = Cargoes,
    {_, CargoCost} = Cargo,
    {_, {_, PrevCost}} = PrevChoice,
    Choice = case CargoCost < PrevCost of
                true -> {1, Cargo};
                false -> PrevChoice
            end,
    compute_min_loss(Cargoes, RestPrevSeq, [Choice]);
compute_min_loss(Cargoes, [PrevChoice|RestPrevSeq], Seq) ->
    [Cargo|_] = Cargoes,
    {CargoWeight, CargoCost} = Cargo,
    [LastChoice|_] = lists:reverse(Seq),
    {_, {Weight, Cost}} = LastChoice,
    {W, {_, PrevCost}} = PrevChoice,
    Choice = case Weight > W of
                true -> {W, {Weight, Cost}};
                false ->
                    case Cost + CargoCost < PrevCost of
                        true -> {W, {Weight+CargoWeight, Cost+CargoCost}};
                        false -> PrevChoice
                    end
            end,
    compute_min_loss(Cargoes, RestPrevSeq, Seq ++ [Choice]).

gcd(A, 0) ->
    A;
gcd(A, B) ->
    gcd(B, A rem B).
