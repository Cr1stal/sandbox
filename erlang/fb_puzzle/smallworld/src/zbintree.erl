-module(zbintree).
-export([root/1, get/1, put/2, right/1, left/1, top/1,
         set_left_branch/2, set_right_branch/2, empty/0]).

% -type node(A) :: undefined
%                | {fork, A, Left::node(A), Right::node(A)}.

% -type choice(A) :: {left, A, node(A)}
%                  | {right, A, node(A)}.

% -type thread(A) :: [choice(A)].

% -type zipper(A) :: {thread(A), node(A)}.

%% Creates a basic binary zipper tree. Should be called first when
%% declaring the data structure
% -spec root(A) -> zipper(A).
root(A) -> {[], {fork, A, undefined, undefined}}.

% -spec empty() -> zipper(_A).
empty() -> {[], undefined}.

%% Fetches the value of the current position
% -spec get(zipper(Val)) -> Val.
get({_Thread, {fork, Val, _Left, _Right}}) -> Val.

%% Either replaces or create a new node (if it was 'undefined')
%% at the current position in the zipper binary tree. The
%% value of the node is the one in the second argument.
% -spec put(A, zipper(A)) -> zipper(A).
put(Val, {Thread, undefined}) ->
    {Thread, {fork, Val, undefined, undefined}};
put(Val, {Thread, {fork, _OldVal, L, R}}) ->
    {Thread, {fork, Val, L, R}}.

%% Moves down the tree one level, picking the right child.
% -spec right(zipper(A)) -> zipper(A).
right({Thread, {fork, Val, L, R}}) -> {[{right, Val, L}|Thread], R}.

%% Moves down the tree one level, picking the left child.
% -spec left(zipper(A)) -> zipper(A).
left({Thread, {fork, Val, L, R}}) -> {[{left, Val, R}|Thread], L}.

%% Moves back up one level. When doing so, it reassembles the
%% Current and Past parts of the trees as a complete node.
% -spec top(zipper(A)) -> zipper(A).
top({[{left, Val, R}|Thread], L}) ->  {Thread, {fork, Val, L, R}};
top({[{right, Val, L}|Thread], R}) -> {Thread, {fork, Val, L, R}}.

%% Shortcut function to add a left child
set_left_branch(A, Zipper) ->
    top(?MODULE:put(A, left(Zipper))).

%% Shortcut function to add a right child
set_right_branch(A, Zipper) ->
    top(?MODULE:put(A, right(Zipper))).

