/* ------------------------------------------------------------------------------
 *
 * Copyright (c) 2018, Lauri Moisio <l@arv.io>
 *
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * ------------------------------------------------------------------------------
 */

 /*
 	When modifying this file, please be extremely thorough when
	it comes to error handling. We don't want to kneecap BEAM if we
	can avoid it.
  */
#include <erl_nif.h>

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define SOCKETFD_CLOSED (1<<0)
#define MAX_TRIES 5
#define INET      4
#define INET6     8

#define ARRAY_SIZE(x) ((sizeof(x))/(sizeof(x[0])))

struct socketfd {
	 // We have to know if we have already closed the enclosed socket,
	 // as the fd value will be reused, and we might unintendedly
	 // close something else and mangle the program
	 //
	 // Also just setting fd to -1 would have worked.
	int flags;
	int fd;
	//struct sockaddr_storage addr;
};

struct error_atom {
	int value;
	const char *atom;
};

static int populate_sockaddr(ErlNifEnv *env, struct sockaddr_storage *addr, ERL_NIF_TERM term, ERL_NIF_TERM *err_ret)
{
	if (!enif_compare(term, enif_make_atom(env, "inet"))) {
		addr->ss_family = AF_INET;
		return 0;
	} else if (!enif_compare(term, enif_make_atom(env, "inet6"))) {
		addr->ss_family = AF_INET6;
		return 0;
	} else if (!enif_compare(term, enif_make_atom(env, "any"))) {
		addr->ss_family = AF_INET6;
		return 0;
	}

	struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
	struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)addr;

	int address_arity;
	const ERL_NIF_TERM *address;
	if (!enif_get_tuple(env, term, &address_arity, &address)) {
		enif_make_badarg(env);
		return -1;
	}

	unsigned int element;
	switch (address_arity) {
	case INET:
		addr->ss_family = AF_INET;
		unsigned int ipv4addr = 0;
		for (int i = 0; i < address_arity; ++i) {
			if (!enif_get_uint(env, address[i], &element) || element > 255) {
				*err_ret = enif_make_tuple2(env, enif_make_atom(env, "bad_address"), term);
				return -1;
			}
			ipv4addr = ipv4addr << 8 | (element & 0xFF);
		}
		addr_in->sin_addr.s_addr = htonl(ipv4addr);
		break;
	case INET6:
		addr->ss_family = AF_INET6;
		for (int i = 0; i < address_arity; ++i) {
			if (!enif_get_uint(env, address[i], &element) || element > 0xFFFF) {
				*err_ret = enif_make_tuple2(env, enif_make_atom(env, "bad_address"), term);
				return -1;
			}
			addr_in6->sin6_addr.s6_addr[(i*2)] = (element >> 8) & 0xFF;
			addr_in6->sin6_addr.s6_addr[(i*2) + 1] = element & 0xFF;
		}
		break;
	default:
		*err_ret = enif_make_tuple2(env, enif_make_atom(env, "bad_address"), term);
		return -1;
	}

	return 0;
}

static ERL_NIF_TERM error_return(ErlNifEnv *env, ERL_NIF_TERM reason)
{
	return enif_make_tuple2(env, enif_make_atom(env, "error"), reason);
}

static int close_socket_fd(int fd)
{
	for (int tried = 0; tried < MAX_TRIES && fd != -1; ++tried) {
		if (!close(fd)) {
			fd = -1;
			break;
		}

		if (errno != EINTR)
			break;
	}

	return !(fd == -1);
}

static ERL_NIF_TERM open_socket(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	if (!enif_compare(argv[1], enif_make_atom(env, "any")))
		return error_return(env, enif_make_atom(env, "invalid_address_family"));

	unsigned int port;
	if (!enif_get_uint(env, argv[2], &port))
		return enif_make_badarg(env);

	if (port >= 0xFFFF)
		return error_return(env, enif_make_atom(env, "invalid_port"));

	int socket_type;
	if (!enif_compare(argv[0], enif_make_atom(env, "stream"))) {
		socket_type = SOCK_STREAM;
	} else if (!enif_compare(argv[0], enif_make_atom(env, "tcp"))) {
		socket_type = SOCK_STREAM;
	} else if (!enif_compare(argv[0], enif_make_atom(env, "datagram"))) {
		socket_type = SOCK_DGRAM;
	} else if (!enif_compare(argv[0], enif_make_atom(env, "udp"))) {
		socket_type = SOCK_DGRAM;;
	} else {
		return error_return(env, enif_make_atom(env, "invalid_socket_type"));
	}

	// Populate sockaddr from the list
	struct sockaddr_storage addr = { .ss_family = AF_INET };
	ERL_NIF_TERM err_ret = argv[1];
	if (populate_sockaddr(env, &addr, argv[1], &err_ret)) {
		if (err_ret != argv[1])
			return error_return(env, err_ret);
		enif_has_pending_exception(env, &err_ret);
		return err_ret;
	}

	socklen_t addrlen = 0;
	switch (addr.ss_family) {
	case AF_INET:
		((struct sockaddr_in *)&addr)->sin_port = htons(((unsigned short)port));
		addrlen = sizeof(struct sockaddr_in);
		break;
	case AF_INET6:
		((struct sockaddr_in6 *)&addr)->sin6_port = htons(((unsigned short)port));
		addrlen = sizeof(struct sockaddr_in6);
		break;
	default:
		return error_return(env, enif_make_atom(env, "invalid_address_family"));
	}

	int fd = socket(addr.ss_family, socket_type, 0);
	if (fd < 0) {
		struct error_atom errors[] = {
			{EACCES,          "eacces"         },
			{EAFNOSUPPORT,    "eafnosupport"   },
			{EMFILE,          "emfile"         },
			{ENFILE,          "enfile"         },
			{ENOBUFS,         "enobufs"        },
			{ENOMEM,          "enomem"         },
			{EPROTONOSUPPORT, "eprotonosupport"},
			{EPROTOTYPE,      "eprototype"     }
		};

		ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
		for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
			if (errno != errors[i].value)
				continue;

			reason = enif_make_atom(env, errors[i].atom);
			break;
		}

		ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "socket"), reason);
		return error_return(env, errtuple);
	}

	if (addr.ss_family == AF_INET6) {
		int curflags, newflags;
		socklen_t optlen = sizeof(curflags);
		if (getsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &curflags, &optlen)) {
			struct error_atom errors[] = {
				{EBADF,       "ebadf"      },
				{EFAULT,      "efault"     },
				{EINVAL,      "einval"     },
				{ENOBUFS,     "enobufs"    },
				{ENOMEM,      "enomem"     },
				{ENOPROTOOPT, "enoprotoopt"},
				{ENOTSOCK,    "enotsock"   }
			};

			ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
			for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
				if (errors[i].value != errno)
					continue;

				reason = enif_make_atom(env, errors[i].atom);
				break;
			}

			if (close_socket_fd(fd))
				enif_raise_exception(env, enif_make_atom(env, "close_socket_error"));
			ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "getsockopt"), reason);
			return error_return(env, errtuple);
		}

		switch (enif_compare(argv[0], enif_make_atom(env, "any"))) {
		case 0:
			// Make ipv6 wildcard into a catchall
			newflags = 0;
			break;
		default:
			newflags = 1;
			break;
		}

		optlen = sizeof(newflags);
		if (newflags != curflags && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &newflags, optlen) != 0) {
			struct error_atom errors[] = {
				{EBADF,       "ebadf"      },
				{EDOM,        "edom"       },
				{EFAULT,      "efault"     },
				{EINVAL,      "einval"     },
				{EISCONN,     "eisconn"    },
				{ENOBUFS,     "enobufs"    },
				{ENOMEM,      "enomem"     },
				{ENOPROTOOPT, "enoprotoopt"},
				{ENOTSOCK,    "enotsock"   }
			};

			ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
			for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
				if (errors[i].value != errno)
					continue;

				reason = enif_make_atom(env, errors[i].atom);
				break;
			}

			if (close_socket_fd(fd))
				enif_raise_exception(env, enif_make_atom(env, "close_socket_error"));
			ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "setsockopt"), reason);
			return error_return(env, errtuple);
		}
	}

	if (bind(fd, (struct sockaddr *)&addr, addrlen)) {
		struct error_atom errors[] = {
			{EACCES,        "eacces"       },
			{EADDRINUSE,    "eaddrinuse"   },
			{EADDRNOTAVAIL, "eaddrnotavail"},
			{EAFNOSUPPORT,  "eafnosupport" },
			{EBADF,         "ebadf"        },
			{EDESTADDRREQ,  "edestaddrreq" },
			{EFAULT,        "efault"       },
			{EINVAL,        "einval"       },
			{ENOTSOCK,      "enotsock"     },
			{EOPNOTSUPP,    "eopnotsupp"   }
		};

		ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
		for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
			if (errors[i].value != errno)
				continue;

			reason = enif_make_atom(env, errors[i].atom);
			break;
		}

		if (close_socket_fd(fd))
			enif_raise_exception(env, enif_make_atom(env, "close_socket_error"));
		ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "bind"), reason);
		return error_return(env, errtuple);
	}

	ErlNifResourceType **type = enif_priv_data(env);

	struct socketfd *fd_obj = enif_alloc_resource(*type, sizeof(struct socketfd));
	if (!fd_obj) {
		if (close_socket_fd(fd))
			enif_raise_exception(env, enif_make_atom(env, "close_socket_error"));
		return error_return(env, enif_make_atom(env, "alloc_resource"));
	}

	fd_obj->fd = fd;
	fd_obj->flags = 0;

	ERL_NIF_TERM obj = enif_make_resource(env, fd_obj);
	enif_release_resource(fd_obj);

	return enif_make_tuple2(env, enif_make_atom(env, "ok"), obj);
}

static ERL_NIF_TERM dup_socket(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifResourceType **type = enif_priv_data(env);

	struct socketfd *fd;
	if (!enif_get_resource(env, argv[0], *type, (void **)&fd))
		return enif_make_badarg(env);

	if (fd->flags & SOCKETFD_CLOSED)
		return error_return(env, enif_make_atom(env, "closed"));

	int tried = 0, newfd;
try_again:
	if ((newfd = dup(fd->fd)) < 0) {
		if (errno == EINTR && tried < MAX_TRIES) {
			tried += 1;
			goto try_again;
		}

		struct error_atom errors[] = {
			{EBADF,  "ebadf" },
			{EINTR,  "eintr" },
			{EMFILE, "emfile"}
		};

		ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
		for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
			if (errors[i].value != errno)
				continue;

			reason = enif_make_atom(env, errors[i].atom);
			break;
		}

		ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "dup"), reason);
		return error_return(env, errtuple);
	}

	struct socketfd *newfd_obj = enif_alloc_resource(*type, sizeof(struct socketfd));
	if (!newfd_obj) {
		if (close_socket_fd(newfd))
			enif_raise_exception(env, enif_make_atom(env, "close_socket_error"));
		return error_return(env, enif_make_atom(env, "alloc_resource"));
	}

	newfd_obj->fd = newfd;
	newfd_obj->flags = 0;
	ERL_NIF_TERM obj = enif_make_resource(env, newfd_obj);
	enif_release_resource(newfd_obj);

	return enif_make_tuple2(env, enif_make_atom(env, "ok"), obj);
}

static ERL_NIF_TERM get_socket(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifResourceType **type = enif_priv_data(env);

	struct socketfd *fd;
	if (!enif_get_resource(env, argv[0], *type, (void **)&fd))
		return enif_make_badarg(env);

	if (fd->flags & SOCKETFD_CLOSED)
		return error_return(env, enif_make_atom(env, "closed"));

	return enif_make_int(env, fd->fd);
}

static ERL_NIF_TERM close_socket(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifResourceType **type = enif_priv_data(env);

	struct socketfd *fd;
	if (!enif_get_resource(env, argv[0], *type, (void **)&fd))
		return enif_make_badarg(env);

	if (fd->flags & SOCKETFD_CLOSED) {
		return enif_make_atom(env, "ok");
	} else if (!close_socket_fd(fd->fd)) {
		fd->flags |= SOCKETFD_CLOSED;
		return enif_make_atom(env, "ok");
	}

	struct error_atom errors[] = {
		{EBADF,  "ebadf"},
		{EINTR,  "eintr"},
		{EIO,    "eio"  }
	};

	ERL_NIF_TERM reason = enif_make_atom(env, "error_placeholder");
	for (int i = 0; i < ARRAY_SIZE(errors); ++i) {
		if (errors[i].value != errno)
			continue;

		reason = enif_make_atom(env, errors[i].atom);
		break;
	}

	ERL_NIF_TERM errtuple = enif_make_tuple2(env, enif_make_atom(env, "close"), reason);
	return error_return(env, errtuple);
}

void destructor(ErlNifEnv *env, struct socketfd *ptr);
void destructor(ErlNifEnv *env, struct socketfd *ptr)
{
	if (!(ptr->flags & SOCKETFD_CLOSED))
		close_socket_fd(ptr->fd);
}

static int load(ErlNifEnv *env, ErlNifResourceType ***priv_data, ERL_NIF_TERM load_info)
{
	*priv_data = enif_alloc(sizeof(ErlNifResourceType *));
	if (!*priv_data)
		return -1;

	ErlNifResourceFlags tried;

	**priv_data = enif_open_resource_type(
		env,
		"socketfd",
		"socketfd",
		(void (*)(ErlNifEnv *, void *))&destructor,
        	ERL_NIF_RT_CREATE,
        	&tried
	);
	if (!**priv_data) {
		enif_free(*priv_data);
		return -1;
	}

	return 0;
}

static ErlNifFunc export[] = {
        {"open",  3, open_socket},
	{"dup",   1, dup_socket},
	{"get",   1, get_socket},
	{"close", 1, close_socket}
};

ERL_NIF_INIT(
        socketfd,
        export,
        (int (*)(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info))&load,
        NULL,
        NULL,
        NULL
)
