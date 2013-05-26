AC_INIT([cql], [0.1], [brent@bantamlabs.com], [cql], [http://github.com/brenttheisen/cql])
AC_PREREQ([2.59])
AM_INIT_AUTOMAKE([1.10 no-define])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([src/config.h])
AC_PROG_CC
AM_PROG_CC_C_O

PKG_CHECK_MODULES([DEPS], [libcqlclient-0.0 >= 0.0.0])
PKG_CHECK_MODULES([LIBEDIT], [libedit],
	[AC_DEFINE([HAVE_LIBEDIT], [1], [Define we have libedit])],
	[AX_LIB_READLINE])
if test "$ac_cv_have_readline" = no; then
	AC_MSG_ERROR([libedit or readline not found])
fi

AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
