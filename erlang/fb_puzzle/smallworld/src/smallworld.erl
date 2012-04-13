-module(smallworld).

%% API
-export([main/1]).

main(Args) ->
    [Filename|_] = Args,
    Friends = read_world(Filename),
    KdTree = build_kdtree(Friends),
    L = [find_nearest(Me, KdTree) || Me <- Friends],
    %% print for facebook puzzle
    [io:format("~B ~B,~B,~B~n", [P1, P2, P3, P4]) || [{P1,_,_}, {P2,_,_}, {P3,_,_}, {P4,_,_}] <- L].

%%==========================================================
%% private
%%==========================================================
read_world(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Friends = string:tokens(binary_to_list(Bin), "\r\n"),
    lists:map(fun(L) ->
                [Id, X, Y] = string:tokens(L, "\s\t"),
                {list_to_integer(Id), list_to_float(X), list_to_float(Y)}
              end, Friends).

build_kdtree([]) ->
    zbintree:empty();
build_kdtree(World) ->
    Sorted = sort_by_axis(World, 0),
    {Median, L, R} = find_list_median(Sorted),
    Tree = zbintree:root(Median),
    build_kdtree(Tree, L, R, 1).
build_kdtree(KdTree, [], [], _) ->
    KdTree;
build_kdtree(KdTree, [Left], [], _) ->
    zbintree:set_left_branch(Left, KdTree);
build_kdtree(KdTree, [], [Right], _) ->
    zbintree:set_right_branch(Right, KdTree);
build_kdtree(KdTree, Left, Right, Depth) ->
    LSorted = sort_by_axis(Left, Depth),
    {LMedian, LLeft, LRight} = find_list_median(LSorted),
    T1 = zbintree:set_left_branch(LMedian, KdTree),
    T2 = build_kdtree(zbintree:left(T1), LLeft, LRight, Depth + 1),
    T3 = zbintree:top(T2),
    RSorted = sort_by_axis(Right, Depth),
    {RMedian, RLeft, RRight} = find_list_median(RSorted),
    T4 = zbintree:set_right_branch(RMedian, T3),
    T5 = build_kdtree(zbintree:right(T4), RLeft, RRight, Depth + 1),
    T6 = zbintree:top(T5),
    T6.

find_nearest(_, {_, undefined}) ->
    [];
find_nearest(Target, KdTree) ->
    Root = zbintree:get(KdTree),
    find_nearest(Target, Root, zbintree:left(KdTree), zbintree:right(KdTree), [], 0).
find_nearest(Target, Leaf, {_, undefined}, {_, undefined}, Nearest, _) ->
    add_to_nearest(Target, Leaf, Nearest);
find_nearest(Target, Root, Left, {_, undefined}, Nearest, Depth) ->
    NewNearest = add_to_nearest(Target, Root, Nearest),
    find_nearest(Target, zbintree:get(Left), zbintree:left(Left), zbintree:right(Left), NewNearest, Depth+1);
find_nearest(Target, Root, {_, undefined}, Right, Nearest, Depth) ->
    NewNearest = add_to_nearest(Target, Root, Nearest),
    find_nearest(Target, zbintree:get(Right), zbintree:left(Right), zbintree:right(Right), NewNearest, Depth+1);
find_nearest(Target, Root, Left, Right, Nearest, Depth) ->
    N = add_to_nearest(Target, Root, Nearest),
    {Near, Away} = select_nearer_by_axis(Target, Root, Left, Right, Depth),
    NN = find_nearest(Target, zbintree:get(Near), zbintree:left(Near), zbintree:right(Near), N, Depth+1),
    %% may need to check the away side too
    case (length(NN) < 4) or (distance_axis(Target, Root, Depth) < distance(Target, lists:last(NN))) of
        true ->
            find_nearest(Target, zbintree:get(Away), zbintree:left(Away), zbintree:right(Away), NN, Depth+1);
        false ->
            NN
    end.

find_list_median([]) ->
    {undefined, [], []};
find_list_median(List) ->
    {Left, [Median|Right]} = lists:split(length(List) div 2, List),
    {Median, Left, Right}.

sort_by_axis(Coords, Depth) ->
    Axis = Depth rem 2,
    lists:sort(fun(E1, E2) ->
        {_, X1, Y1} = E1,
        {_, X2, Y2} = E2,
        case Axis of
            0 -> X1 =< X2;
            1 -> Y1 =< Y2
        end
    end, Coords).

select_nearer_by_axis(Target, Root, Left, Right, Depth) ->
    Axis = Depth rem 2,
    {_, X, Y} = Target,
    {_, X1, Y1} = Root,
    case Axis of
        0 ->
            case X =< X1 of
                true -> {Left, Right};
                false -> {Right, Left}
            end;
        1 ->
            case Y =< Y1 of
                true -> {Left, Right};
                false -> {Right, Left}
            end
    end.

add_to_nearest(_, Coord, []) ->
    [Coord];
add_to_nearest(Target, Coord, CoordList) ->
    case length(CoordList) < 4 of
        true ->
            sort_by_distance(Target, [Coord | CoordList]);
        false ->
            case distance(Target, Coord) < distance(Target, lists:last(CoordList)) of
                true ->
                    {HeadList, _} = lists:split(length(CoordList) - 1, CoordList),
                    sort_by_distance(Target, [Coord | HeadList]);
                false ->
                    CoordList
            end
    end.

distance(Coord1, Coord2) ->
    {_, X1, Y1} = Coord1,
    {_, X2, Y2} = Coord2,
    (X1 - X2)*(X1 - X2) + (Y1 - Y2)*(Y1-Y2).

distance_axis(Coord1, Coord2, Depth) ->
    {_, X1, Y1} = Coord1,
    {_, X2, Y2} = Coord2,
    Axis = Depth rem 2,
    case Axis of
        0 -> (X1 - X2)*(X1 - X2);
        1 -> (Y1 - Y2)*(Y1 - Y2)
    end.

sort_by_distance(Target, List) ->
    lists:sort(fun(E1, E2) ->
                    distance(Target, E1) =< distance(Target, E2)
                end, List).
