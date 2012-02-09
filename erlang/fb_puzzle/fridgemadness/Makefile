all: compile

clean:
	@find . -name \*.beam | xargs rm -f

compile:
	@erlc -o ebin src/*.erl
