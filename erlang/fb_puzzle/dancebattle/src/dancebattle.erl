-module(dancebattle).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    {N, MovesTaken, LastPlayer, LastMove} = read_input(Filename),
    Winner = play_to_end(N, MovesTaken, LastPlayer, LastMove),
    case Winner of
        1 ->
            io:format("Win~n");
        2 ->
            io:format("Lose~n")
    end.


%%===================================================================
%% private
%%===================================================================
read_input(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Input = string:tokens(binary_to_list(Bin), "\r\n"),
    N = list_to_integer(hd(Input)),
    M = list_to_integer(hd(tl(Input))),
    {MovesTaken, LastPlayer, LastMove} = read_moves(tl(tl(Input)), M),

    {N, MovesTaken, LastPlayer, LastMove}.

read_moves(Input, M) ->
    read_moves(Input, M, [], 1, 0).
read_moves(_Input, 0, Moves, LastPlayer, LastMove) ->
    {Moves, LastPlayer, LastMove};
read_moves(Input, M, Moves, LastPlayer, _) ->
    Line = hd(Input),
    [Move1, Move2] = string:tokens(Line, "\s\t"),
    NewMoves =  add_move({list_to_integer(Move1), list_to_integer(Move2)}, Moves),
    Player = next_player(LastPlayer),
    read_moves(tl(Input), M - 1, NewMoves, Player, list_to_integer(Move2)).

play_to_end(N, MovesTaken, LastPlayer, LastMove) ->
    NextMoves = next_moves(N, MovesTaken, LastMove),
    NextPlayer = next_player(LastPlayer),
    play(N, MovesTaken, [{NextPlayer, NextMoves}], []).

play(_N, _MovesTaken, [{_, []}], _) ->
    %% exhausted, player 2 wins
    2;
play(N, MovesTaken, [{_, []} | Rest], MovesPlayed) ->
    play(N, MovesTaken, Rest, MovesPlayed);
play(N, MovesTaken, MovesSeq, MovesPlayed) ->
    [{Player, [{Move1, Move2} | RestMoves]} | Rest] = MovesSeq,
    NewMovesPlayed = add_move({Move1, Move2}, MovesPlayed),
    NextMoves = next_moves(N, MovesTaken ++ NewMovesPlayed, Move2),
    NextPlayer = next_player(Player),
    case {NextPlayer, NextMoves} of
        {1, []} ->
            %% player 1 will lose, try current player's next move
            play(N, MovesTaken, [{Player, RestMoves} | Rest], MovesPlayed);
        {2, []} ->
            %% player 1 wins!
            1;
        _ ->
            %% Player playes {Move1, Move2}, now it's next player's turn
            Seq = [{Player, RestMoves} | Rest],
            play(N, MovesTaken, [{NextPlayer, NextMoves} | Seq], NewMovesPlayed)
    end.

add_move({Move1, Move2}, Moves) ->
    NewMoves =  case Move1 =:= Move2 of
                    true -> [{Move1, Move2} | Moves];
                    false ->
                        lists:append(Moves, [{Move1, Move2}, {Move2, Move1}])
                end,
    NewMoves.

next_moves(N, MovesTaken, LastMove) ->
    AllMoves = [{LastMove, Move}|| Move <- lists:seq(0, N - 1)],
    lists:filter(fun(E) -> not lists:member(E, MovesTaken) end, AllMoves).

next_player(Player) ->
    Player rem 2 + 1.
