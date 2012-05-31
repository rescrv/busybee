#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.66])
AC_INIT([BusyBee], [0.1.0], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-bzip2 dist-lzma])
AM_SILENT_RULES([yes])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([busybee.cc])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG(C++)

ANAL_WARNINGS_CXX

# Checks for libraries.
PKG_CHECK_MODULES([PO6], [libpo6 >= 0.2.3])
PKG_CHECK_MODULES([E], [libe >= 0.2.7])

# Checks for header files.
AC_CHECK_HEADERS([netinet/in.h stdint.h stdlib.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MMAP
AC_CHECK_FUNCS([alarm clock_gettime ftruncate memmove mkdir munmap rmdir socket])

# Optional components

AC_CONFIG_FILES([Makefile
                 busybee.pc])
AC_OUTPUT
