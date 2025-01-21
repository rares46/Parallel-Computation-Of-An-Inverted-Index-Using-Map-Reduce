build:
		gcc -fsanitize=address -g main.c -o tema1 -lpthread
clean:
		rm tema1