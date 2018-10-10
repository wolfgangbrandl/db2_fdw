# 1 "db2_utils.c"
# 1 "/home/postgres1/contrib/db2_fdw//"
# 1 "<built-in>"
# 1 "<command-line>"
# 1 "/usr/include/stdc-predef.h" 1 3 4
# 1 "<command-line>" 2
# 1 "db2_utils.c"
# 9 "db2_utils.c"
# 1 "/usr/include/string.h" 1 3 4
# 25 "/usr/include/string.h" 3 4
# 1 "/usr/include/features.h" 1 3 4
# 375 "/usr/include/features.h" 3 4
# 1 "/usr/include/sys/cdefs.h" 1 3 4
# 392 "/usr/include/sys/cdefs.h" 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 393 "/usr/include/sys/cdefs.h" 2 3 4
# 376 "/usr/include/features.h" 2 3 4
# 399 "/usr/include/features.h" 3 4
# 1 "/usr/include/gnu/stubs.h" 1 3 4
# 10 "/usr/include/gnu/stubs.h" 3 4
# 1 "/usr/include/gnu/stubs-64.h" 1 3 4
# 11 "/usr/include/gnu/stubs.h" 2 3 4
# 400 "/usr/include/features.h" 2 3 4
# 26 "/usr/include/string.h" 2 3 4






# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 212 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 3 4
typedef long unsigned int size_t;
# 33 "/usr/include/string.h" 2 3 4









extern void *memcpy (void *__restrict __dest, const void *__restrict __src,
       size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern void *memmove (void *__dest, const void *__src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));






extern void *memccpy (void *__restrict __dest, const void *__restrict __src,
        int __c, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));





extern void *memset (void *__s, int __c, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern int memcmp (const void *__s1, const void *__s2, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));
# 92 "/usr/include/string.h" 3 4
extern void *memchr (const void *__s, int __c, size_t __n)
      __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));


# 106 "/usr/include/string.h" 3 4
extern void *rawmemchr (const void *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));
# 117 "/usr/include/string.h" 3 4
extern void *memrchr (const void *__s, int __c, size_t __n)
      __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));






extern char *strcpy (char *__restrict __dest, const char *__restrict __src)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));

extern char *strncpy (char *__restrict __dest,
        const char *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern char *strcat (char *__restrict __dest, const char *__restrict __src)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));

extern char *strncat (char *__restrict __dest, const char *__restrict __src,
        size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern int strcmp (const char *__s1, const char *__s2)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));

extern int strncmp (const char *__s1, const char *__s2, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));


extern int strcoll (const char *__s1, const char *__s2)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));

extern size_t strxfrm (char *__restrict __dest,
         const char *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));






# 1 "/usr/include/xlocale.h" 1 3 4
# 27 "/usr/include/xlocale.h" 3 4
typedef struct __locale_struct
{

  struct __locale_data *__locales[13];


  const unsigned short int *__ctype_b;
  const int *__ctype_tolower;
  const int *__ctype_toupper;


  const char *__names[13];
} *__locale_t;


typedef __locale_t locale_t;
# 160 "/usr/include/string.h" 2 3 4


extern int strcoll_l (const char *__s1, const char *__s2, __locale_t __l)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2, 3)));

extern size_t strxfrm_l (char *__dest, const char *__src, size_t __n,
    __locale_t __l) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 4)));





extern char *strdup (const char *__s)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) __attribute__ ((__nonnull__ (1)));






extern char *strndup (const char *__string, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) __attribute__ ((__nonnull__ (1)));
# 207 "/usr/include/string.h" 3 4

# 232 "/usr/include/string.h" 3 4
extern char *strchr (const char *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));
# 259 "/usr/include/string.h" 3 4
extern char *strrchr (const char *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));


# 273 "/usr/include/string.h" 3 4
extern char *strchrnul (const char *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));






extern size_t strcspn (const char *__s, const char *__reject)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));


extern size_t strspn (const char *__s, const char *__accept)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));
# 311 "/usr/include/string.h" 3 4
extern char *strpbrk (const char *__s, const char *__accept)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));
# 338 "/usr/include/string.h" 3 4
extern char *strstr (const char *__haystack, const char *__needle)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));




extern char *strtok (char *__restrict __s, const char *__restrict __delim)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));




extern char *__strtok_r (char *__restrict __s,
    const char *__restrict __delim,
    char **__restrict __save_ptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 3)));

extern char *strtok_r (char *__restrict __s, const char *__restrict __delim,
         char **__restrict __save_ptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 3)));
# 369 "/usr/include/string.h" 3 4
extern char *strcasestr (const char *__haystack, const char *__needle)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));







extern void *memmem (const void *__haystack, size_t __haystacklen,
       const void *__needle, size_t __needlelen)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 3)));



extern void *__mempcpy (void *__restrict __dest,
   const void *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern void *mempcpy (void *__restrict __dest,
        const void *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));





extern size_t strlen (const char *__s)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));





extern size_t strnlen (const char *__string, size_t __maxlen)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));





extern char *strerror (int __errnum) __attribute__ ((__nothrow__ , __leaf__));

# 434 "/usr/include/string.h" 3 4
extern char *strerror_r (int __errnum, char *__buf, size_t __buflen)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2))) ;





extern char *strerror_l (int __errnum, __locale_t __l) __attribute__ ((__nothrow__ , __leaf__));





extern void __bzero (void *__s, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));



extern void bcopy (const void *__src, void *__dest, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern void bzero (void *__s, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern int bcmp (const void *__s1, const void *__s2, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));
# 485 "/usr/include/string.h" 3 4
extern char *index (const char *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));
# 513 "/usr/include/string.h" 3 4
extern char *rindex (const char *__s, int __c)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1)));




extern int ffs (int __i) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));




extern int ffsl (long int __l) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));

__extension__ extern int ffsll (long long int __ll)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));




extern int strcasecmp (const char *__s1, const char *__s2)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));


extern int strncasecmp (const char *__s1, const char *__s2, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));





extern int strcasecmp_l (const char *__s1, const char *__s2,
    __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2, 3)));

extern int strncasecmp_l (const char *__s1, const char *__s2,
     size_t __n, __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2, 4)));





extern char *strsep (char **__restrict __stringp,
       const char *__restrict __delim)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));




extern char *strsignal (int __sig) __attribute__ ((__nothrow__ , __leaf__));


extern char *__stpcpy (char *__restrict __dest, const char *__restrict __src)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern char *stpcpy (char *__restrict __dest, const char *__restrict __src)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));



extern char *__stpncpy (char *__restrict __dest,
   const char *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern char *stpncpy (char *__restrict __dest,
        const char *__restrict __src, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));




extern int strverscmp (const char *__s1, const char *__s2)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1, 2)));


extern char *strfry (char *__string) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern void *memfrob (void *__s, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));
# 602 "/usr/include/string.h" 3 4
extern char *basename (const char *__filename) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));
# 642 "/usr/include/string.h" 3 4

# 10 "db2_utils.c" 2
# 1 "/usr/include/stdlib.h" 1 3 4
# 32 "/usr/include/stdlib.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 324 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 3 4
typedef int wchar_t;
# 33 "/usr/include/stdlib.h" 2 3 4








# 1 "/usr/include/bits/waitflags.h" 1 3 4
# 42 "/usr/include/stdlib.h" 2 3 4
# 1 "/usr/include/bits/waitstatus.h" 1 3 4
# 64 "/usr/include/bits/waitstatus.h" 3 4
# 1 "/usr/include/endian.h" 1 3 4
# 36 "/usr/include/endian.h" 3 4
# 1 "/usr/include/bits/endian.h" 1 3 4
# 37 "/usr/include/endian.h" 2 3 4
# 60 "/usr/include/endian.h" 3 4
# 1 "/usr/include/bits/byteswap.h" 1 3 4
# 27 "/usr/include/bits/byteswap.h" 3 4
# 1 "/usr/include/bits/types.h" 1 3 4
# 27 "/usr/include/bits/types.h" 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 28 "/usr/include/bits/types.h" 2 3 4


typedef unsigned char __u_char;
typedef unsigned short int __u_short;
typedef unsigned int __u_int;
typedef unsigned long int __u_long;


typedef signed char __int8_t;
typedef unsigned char __uint8_t;
typedef signed short int __int16_t;
typedef unsigned short int __uint16_t;
typedef signed int __int32_t;
typedef unsigned int __uint32_t;

typedef signed long int __int64_t;
typedef unsigned long int __uint64_t;







typedef long int __quad_t;
typedef unsigned long int __u_quad_t;
# 130 "/usr/include/bits/types.h" 3 4
# 1 "/usr/include/bits/typesizes.h" 1 3 4
# 131 "/usr/include/bits/types.h" 2 3 4


typedef unsigned long int __dev_t;
typedef unsigned int __uid_t;
typedef unsigned int __gid_t;
typedef unsigned long int __ino_t;
typedef unsigned long int __ino64_t;
typedef unsigned int __mode_t;
typedef unsigned long int __nlink_t;
typedef long int __off_t;
typedef long int __off64_t;
typedef int __pid_t;
typedef struct { int __val[2]; } __fsid_t;
typedef long int __clock_t;
typedef unsigned long int __rlim_t;
typedef unsigned long int __rlim64_t;
typedef unsigned int __id_t;
typedef long int __time_t;
typedef unsigned int __useconds_t;
typedef long int __suseconds_t;

typedef int __daddr_t;
typedef int __key_t;


typedef int __clockid_t;


typedef void * __timer_t;


typedef long int __blksize_t;




typedef long int __blkcnt_t;
typedef long int __blkcnt64_t;


typedef unsigned long int __fsblkcnt_t;
typedef unsigned long int __fsblkcnt64_t;


typedef unsigned long int __fsfilcnt_t;
typedef unsigned long int __fsfilcnt64_t;


typedef long int __fsword_t;

typedef long int __ssize_t;


typedef long int __syscall_slong_t;

typedef unsigned long int __syscall_ulong_t;



typedef __off64_t __loff_t;
typedef __quad_t *__qaddr_t;
typedef char *__caddr_t;


typedef long int __intptr_t;


typedef unsigned int __socklen_t;
# 28 "/usr/include/bits/byteswap.h" 2 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 29 "/usr/include/bits/byteswap.h" 2 3 4






# 1 "/usr/include/bits/byteswap-16.h" 1 3 4
# 36 "/usr/include/bits/byteswap.h" 2 3 4
# 44 "/usr/include/bits/byteswap.h" 3 4
static __inline unsigned int
__bswap_32 (unsigned int __bsx)
{
  return __builtin_bswap32 (__bsx);
}
# 108 "/usr/include/bits/byteswap.h" 3 4
static __inline __uint64_t
__bswap_64 (__uint64_t __bsx)
{
  return __builtin_bswap64 (__bsx);
}
# 61 "/usr/include/endian.h" 2 3 4
# 65 "/usr/include/bits/waitstatus.h" 2 3 4

union wait
  {
    int w_status;
    struct
      {

 unsigned int __w_termsig:7;
 unsigned int __w_coredump:1;
 unsigned int __w_retcode:8;
 unsigned int:16;







      } __wait_terminated;
    struct
      {

 unsigned int __w_stopval:8;
 unsigned int __w_stopsig:8;
 unsigned int:16;






      } __wait_stopped;
  };
# 43 "/usr/include/stdlib.h" 2 3 4
# 67 "/usr/include/stdlib.h" 3 4
typedef union
  {
    union wait *__uptr;
    int *__iptr;
  } __WAIT_STATUS __attribute__ ((__transparent_union__));
# 95 "/usr/include/stdlib.h" 3 4


typedef struct
  {
    int quot;
    int rem;
  } div_t;



typedef struct
  {
    long int quot;
    long int rem;
  } ldiv_t;







__extension__ typedef struct
  {
    long long int quot;
    long long int rem;
  } lldiv_t;


# 139 "/usr/include/stdlib.h" 3 4
extern size_t __ctype_get_mb_cur_max (void) __attribute__ ((__nothrow__ , __leaf__)) ;




extern double atof (const char *__nptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1))) ;

extern int atoi (const char *__nptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1))) ;

extern long int atol (const char *__nptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1))) ;





__extension__ extern long long int atoll (const char *__nptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1))) ;





extern double strtod (const char *__restrict __nptr,
        char **__restrict __endptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





extern float strtof (const char *__restrict __nptr,
       char **__restrict __endptr) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));

extern long double strtold (const char *__restrict __nptr,
       char **__restrict __endptr)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





extern long int strtol (const char *__restrict __nptr,
   char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));

extern unsigned long int strtoul (const char *__restrict __nptr,
      char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));




__extension__
extern long long int strtoq (const char *__restrict __nptr,
        char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));

__extension__
extern unsigned long long int strtouq (const char *__restrict __nptr,
           char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





__extension__
extern long long int strtoll (const char *__restrict __nptr,
         char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));

__extension__
extern unsigned long long int strtoull (const char *__restrict __nptr,
     char **__restrict __endptr, int __base)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));

# 239 "/usr/include/stdlib.h" 3 4
extern long int strtol_l (const char *__restrict __nptr,
     char **__restrict __endptr, int __base,
     __locale_t __loc) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 4)));

extern unsigned long int strtoul_l (const char *__restrict __nptr,
        char **__restrict __endptr,
        int __base, __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 4)));

__extension__
extern long long int strtoll_l (const char *__restrict __nptr,
    char **__restrict __endptr, int __base,
    __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 4)));

__extension__
extern unsigned long long int strtoull_l (const char *__restrict __nptr,
       char **__restrict __endptr,
       int __base, __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 4)));

extern double strtod_l (const char *__restrict __nptr,
   char **__restrict __endptr, __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 3)));

extern float strtof_l (const char *__restrict __nptr,
         char **__restrict __endptr, __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 3)));

extern long double strtold_l (const char *__restrict __nptr,
         char **__restrict __endptr,
         __locale_t __loc)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 3)));
# 305 "/usr/include/stdlib.h" 3 4
extern char *l64a (long int __n) __attribute__ ((__nothrow__ , __leaf__)) ;


extern long int a64l (const char *__s)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__pure__)) __attribute__ ((__nonnull__ (1))) ;




# 1 "/usr/include/sys/types.h" 1 3 4
# 27 "/usr/include/sys/types.h" 3 4






typedef __u_char u_char;
typedef __u_short u_short;
typedef __u_int u_int;
typedef __u_long u_long;
typedef __quad_t quad_t;
typedef __u_quad_t u_quad_t;
typedef __fsid_t fsid_t;




typedef __loff_t loff_t;



typedef __ino_t ino_t;






typedef __ino64_t ino64_t;




typedef __dev_t dev_t;




typedef __gid_t gid_t;




typedef __mode_t mode_t;




typedef __nlink_t nlink_t;




typedef __uid_t uid_t;





typedef __off_t off_t;






typedef __off64_t off64_t;




typedef __pid_t pid_t;





typedef __id_t id_t;




typedef __ssize_t ssize_t;





typedef __daddr_t daddr_t;
typedef __caddr_t caddr_t;





typedef __key_t key_t;
# 132 "/usr/include/sys/types.h" 3 4
# 1 "/usr/include/time.h" 1 3 4
# 57 "/usr/include/time.h" 3 4


typedef __clock_t clock_t;



# 73 "/usr/include/time.h" 3 4


typedef __time_t time_t;



# 91 "/usr/include/time.h" 3 4
typedef __clockid_t clockid_t;
# 103 "/usr/include/time.h" 3 4
typedef __timer_t timer_t;
# 133 "/usr/include/sys/types.h" 2 3 4



typedef __useconds_t useconds_t;



typedef __suseconds_t suseconds_t;





# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 147 "/usr/include/sys/types.h" 2 3 4



typedef unsigned long int ulong;
typedef unsigned short int ushort;
typedef unsigned int uint;
# 194 "/usr/include/sys/types.h" 3 4
typedef int int8_t __attribute__ ((__mode__ (__QI__)));
typedef int int16_t __attribute__ ((__mode__ (__HI__)));
typedef int int32_t __attribute__ ((__mode__ (__SI__)));
typedef int int64_t __attribute__ ((__mode__ (__DI__)));


typedef unsigned int u_int8_t __attribute__ ((__mode__ (__QI__)));
typedef unsigned int u_int16_t __attribute__ ((__mode__ (__HI__)));
typedef unsigned int u_int32_t __attribute__ ((__mode__ (__SI__)));
typedef unsigned int u_int64_t __attribute__ ((__mode__ (__DI__)));

typedef int register_t __attribute__ ((__mode__ (__word__)));
# 219 "/usr/include/sys/types.h" 3 4
# 1 "/usr/include/sys/select.h" 1 3 4
# 30 "/usr/include/sys/select.h" 3 4
# 1 "/usr/include/bits/select.h" 1 3 4
# 22 "/usr/include/bits/select.h" 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 23 "/usr/include/bits/select.h" 2 3 4
# 31 "/usr/include/sys/select.h" 2 3 4


# 1 "/usr/include/bits/sigset.h" 1 3 4
# 23 "/usr/include/bits/sigset.h" 3 4
typedef int __sig_atomic_t;




typedef struct
  {
    unsigned long int __val[(1024 / (8 * sizeof (unsigned long int)))];
  } __sigset_t;
# 34 "/usr/include/sys/select.h" 2 3 4



typedef __sigset_t sigset_t;





# 1 "/usr/include/time.h" 1 3 4
# 120 "/usr/include/time.h" 3 4
struct timespec
  {
    __time_t tv_sec;
    __syscall_slong_t tv_nsec;
  };
# 44 "/usr/include/sys/select.h" 2 3 4

# 1 "/usr/include/bits/time.h" 1 3 4
# 30 "/usr/include/bits/time.h" 3 4
struct timeval
  {
    __time_t tv_sec;
    __suseconds_t tv_usec;
  };
# 46 "/usr/include/sys/select.h" 2 3 4
# 54 "/usr/include/sys/select.h" 3 4
typedef long int __fd_mask;
# 64 "/usr/include/sys/select.h" 3 4
typedef struct
  {



    __fd_mask fds_bits[1024 / (8 * (int) sizeof (__fd_mask))];





  } fd_set;






typedef __fd_mask fd_mask;
# 96 "/usr/include/sys/select.h" 3 4

# 106 "/usr/include/sys/select.h" 3 4
extern int select (int __nfds, fd_set *__restrict __readfds,
     fd_set *__restrict __writefds,
     fd_set *__restrict __exceptfds,
     struct timeval *__restrict __timeout);
# 118 "/usr/include/sys/select.h" 3 4
extern int pselect (int __nfds, fd_set *__restrict __readfds,
      fd_set *__restrict __writefds,
      fd_set *__restrict __exceptfds,
      const struct timespec *__restrict __timeout,
      const __sigset_t *__restrict __sigmask);
# 131 "/usr/include/sys/select.h" 3 4

# 220 "/usr/include/sys/types.h" 2 3 4


# 1 "/usr/include/sys/sysmacros.h" 1 3 4
# 29 "/usr/include/sys/sysmacros.h" 3 4


__extension__
extern unsigned int gnu_dev_major (unsigned long long int __dev)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));
__extension__
extern unsigned int gnu_dev_minor (unsigned long long int __dev)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));
__extension__
extern unsigned long long int gnu_dev_makedev (unsigned int __major,
            unsigned int __minor)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));
# 63 "/usr/include/sys/sysmacros.h" 3 4

# 223 "/usr/include/sys/types.h" 2 3 4





typedef __blksize_t blksize_t;






typedef __blkcnt_t blkcnt_t;



typedef __fsblkcnt_t fsblkcnt_t;



typedef __fsfilcnt_t fsfilcnt_t;
# 262 "/usr/include/sys/types.h" 3 4
typedef __blkcnt64_t blkcnt64_t;
typedef __fsblkcnt64_t fsblkcnt64_t;
typedef __fsfilcnt64_t fsfilcnt64_t;





# 1 "/usr/include/bits/pthreadtypes.h" 1 3 4
# 21 "/usr/include/bits/pthreadtypes.h" 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 22 "/usr/include/bits/pthreadtypes.h" 2 3 4
# 60 "/usr/include/bits/pthreadtypes.h" 3 4
typedef unsigned long int pthread_t;


union pthread_attr_t
{
  char __size[56];
  long int __align;
};

typedef union pthread_attr_t pthread_attr_t;





typedef struct __pthread_internal_list
{
  struct __pthread_internal_list *__prev;
  struct __pthread_internal_list *__next;
} __pthread_list_t;
# 90 "/usr/include/bits/pthreadtypes.h" 3 4
typedef union
{
  struct __pthread_mutex_s
  {
    int __lock;
    unsigned int __count;
    int __owner;

    unsigned int __nusers;



    int __kind;

    short __spins;
    short __elision;
    __pthread_list_t __list;
# 124 "/usr/include/bits/pthreadtypes.h" 3 4
  } __data;
  char __size[40];
  long int __align;
} pthread_mutex_t;

typedef union
{
  char __size[4];
  int __align;
} pthread_mutexattr_t;




typedef union
{
  struct
  {
    int __lock;
    unsigned int __futex;
    __extension__ unsigned long long int __total_seq;
    __extension__ unsigned long long int __wakeup_seq;
    __extension__ unsigned long long int __woken_seq;
    void *__mutex;
    unsigned int __nwaiters;
    unsigned int __broadcast_seq;
  } __data;
  char __size[48];
  __extension__ long long int __align;
} pthread_cond_t;

typedef union
{
  char __size[4];
  int __align;
} pthread_condattr_t;



typedef unsigned int pthread_key_t;



typedef int pthread_once_t;





typedef union
{

  struct
  {
    int __lock;
    unsigned int __nr_readers;
    unsigned int __readers_wakeup;
    unsigned int __writer_wakeup;
    unsigned int __nr_readers_queued;
    unsigned int __nr_writers_queued;
    int __writer;
    int __shared;
    unsigned long int __pad1;
    unsigned long int __pad2;


    unsigned int __flags;

  } __data;
# 211 "/usr/include/bits/pthreadtypes.h" 3 4
  char __size[56];
  long int __align;
} pthread_rwlock_t;

typedef union
{
  char __size[8];
  long int __align;
} pthread_rwlockattr_t;





typedef volatile int pthread_spinlock_t;




typedef union
{
  char __size[32];
  long int __align;
} pthread_barrier_t;

typedef union
{
  char __size[4];
  int __align;
} pthread_barrierattr_t;
# 271 "/usr/include/sys/types.h" 2 3 4



# 315 "/usr/include/stdlib.h" 2 3 4






extern long int random (void) __attribute__ ((__nothrow__ , __leaf__));


extern void srandom (unsigned int __seed) __attribute__ ((__nothrow__ , __leaf__));





extern char *initstate (unsigned int __seed, char *__statebuf,
   size_t __statelen) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));



extern char *setstate (char *__statebuf) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));







struct random_data
  {
    int32_t *fptr;
    int32_t *rptr;
    int32_t *state;
    int rand_type;
    int rand_deg;
    int rand_sep;
    int32_t *end_ptr;
  };

extern int random_r (struct random_data *__restrict __buf,
       int32_t *__restrict __result) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));

extern int srandom_r (unsigned int __seed, struct random_data *__buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));

extern int initstate_r (unsigned int __seed, char *__restrict __statebuf,
   size_t __statelen,
   struct random_data *__restrict __buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 4)));

extern int setstate_r (char *__restrict __statebuf,
         struct random_data *__restrict __buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));






extern int rand (void) __attribute__ ((__nothrow__ , __leaf__));

extern void srand (unsigned int __seed) __attribute__ ((__nothrow__ , __leaf__));




extern int rand_r (unsigned int *__seed) __attribute__ ((__nothrow__ , __leaf__));







extern double drand48 (void) __attribute__ ((__nothrow__ , __leaf__));
extern double erand48 (unsigned short int __xsubi[3]) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern long int lrand48 (void) __attribute__ ((__nothrow__ , __leaf__));
extern long int nrand48 (unsigned short int __xsubi[3])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern long int mrand48 (void) __attribute__ ((__nothrow__ , __leaf__));
extern long int jrand48 (unsigned short int __xsubi[3])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern void srand48 (long int __seedval) __attribute__ ((__nothrow__ , __leaf__));
extern unsigned short int *seed48 (unsigned short int __seed16v[3])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));
extern void lcong48 (unsigned short int __param[7]) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





struct drand48_data
  {
    unsigned short int __x[3];
    unsigned short int __old_x[3];
    unsigned short int __c;
    unsigned short int __init;
    unsigned long long int __a;
  };


extern int drand48_r (struct drand48_data *__restrict __buffer,
        double *__restrict __result) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern int erand48_r (unsigned short int __xsubi[3],
        struct drand48_data *__restrict __buffer,
        double *__restrict __result) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern int lrand48_r (struct drand48_data *__restrict __buffer,
        long int *__restrict __result)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern int nrand48_r (unsigned short int __xsubi[3],
        struct drand48_data *__restrict __buffer,
        long int *__restrict __result)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern int mrand48_r (struct drand48_data *__restrict __buffer,
        long int *__restrict __result)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
extern int jrand48_r (unsigned short int __xsubi[3],
        struct drand48_data *__restrict __buffer,
        long int *__restrict __result)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));


extern int srand48_r (long int __seedval, struct drand48_data *__buffer)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));

extern int seed48_r (unsigned short int __seed16v[3],
       struct drand48_data *__buffer) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));

extern int lcong48_r (unsigned short int __param[7],
        struct drand48_data *__buffer)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));









extern void *malloc (size_t __size) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) ;

extern void *calloc (size_t __nmemb, size_t __size)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) ;










extern void *realloc (void *__ptr, size_t __size)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__warn_unused_result__));

extern void free (void *__ptr) __attribute__ ((__nothrow__ , __leaf__));




extern void cfree (void *__ptr) __attribute__ ((__nothrow__ , __leaf__));



# 1 "/usr/include/alloca.h" 1 3 4
# 24 "/usr/include/alloca.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 25 "/usr/include/alloca.h" 2 3 4







extern void *alloca (size_t __size) __attribute__ ((__nothrow__ , __leaf__));






# 492 "/usr/include/stdlib.h" 2 3 4





extern void *valloc (size_t __size) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) ;




extern int posix_memalign (void **__memptr, size_t __alignment, size_t __size)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;




extern void *aligned_alloc (size_t __alignment, size_t __size)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__, __alloc_size__ (2)));




extern void abort (void) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__noreturn__));



extern int atexit (void (*__func) (void)) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));







extern int at_quick_exit (void (*__func) (void)) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));







extern int on_exit (void (*__func) (int __status, void *__arg), void *__arg)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));






extern void exit (int __status) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__noreturn__));





extern void quick_exit (int __status) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__noreturn__));







extern void _Exit (int __status) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__noreturn__));






extern char *getenv (const char *__name) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;





extern char *secure_getenv (const char *__name)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;






extern int putenv (char *__string) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





extern int setenv (const char *__name, const char *__value, int __replace)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));


extern int unsetenv (const char *__name) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));






extern int clearenv (void) __attribute__ ((__nothrow__ , __leaf__));
# 605 "/usr/include/stdlib.h" 3 4
extern char *mktemp (char *__template) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));
# 619 "/usr/include/stdlib.h" 3 4
extern int mkstemp (char *__template) __attribute__ ((__nonnull__ (1))) ;
# 629 "/usr/include/stdlib.h" 3 4
extern int mkstemp64 (char *__template) __attribute__ ((__nonnull__ (1))) ;
# 641 "/usr/include/stdlib.h" 3 4
extern int mkstemps (char *__template, int __suffixlen) __attribute__ ((__nonnull__ (1))) ;
# 651 "/usr/include/stdlib.h" 3 4
extern int mkstemps64 (char *__template, int __suffixlen)
     __attribute__ ((__nonnull__ (1))) ;
# 662 "/usr/include/stdlib.h" 3 4
extern char *mkdtemp (char *__template) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
# 673 "/usr/include/stdlib.h" 3 4
extern int mkostemp (char *__template, int __flags) __attribute__ ((__nonnull__ (1))) ;
# 683 "/usr/include/stdlib.h" 3 4
extern int mkostemp64 (char *__template, int __flags) __attribute__ ((__nonnull__ (1))) ;
# 693 "/usr/include/stdlib.h" 3 4
extern int mkostemps (char *__template, int __suffixlen, int __flags)
     __attribute__ ((__nonnull__ (1))) ;
# 705 "/usr/include/stdlib.h" 3 4
extern int mkostemps64 (char *__template, int __suffixlen, int __flags)
     __attribute__ ((__nonnull__ (1))) ;









extern int system (const char *__command) ;






extern char *canonicalize_file_name (const char *__name)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
# 733 "/usr/include/stdlib.h" 3 4
extern char *realpath (const char *__restrict __name,
         char *__restrict __resolved) __attribute__ ((__nothrow__ , __leaf__)) ;






typedef int (*__compar_fn_t) (const void *, const void *);


typedef __compar_fn_t comparison_fn_t;



typedef int (*__compar_d_fn_t) (const void *, const void *, void *);





extern void *bsearch (const void *__key, const void *__base,
        size_t __nmemb, size_t __size, __compar_fn_t __compar)
     __attribute__ ((__nonnull__ (1, 2, 5))) ;



extern void qsort (void *__base, size_t __nmemb, size_t __size,
     __compar_fn_t __compar) __attribute__ ((__nonnull__ (1, 4)));

extern void qsort_r (void *__base, size_t __nmemb, size_t __size,
       __compar_d_fn_t __compar, void *__arg)
  __attribute__ ((__nonnull__ (1, 4)));




extern int abs (int __x) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;
extern long int labs (long int __x) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;



__extension__ extern long long int llabs (long long int __x)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;







extern div_t div (int __numer, int __denom)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;
extern ldiv_t ldiv (long int __numer, long int __denom)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;




__extension__ extern lldiv_t lldiv (long long int __numer,
        long long int __denom)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__)) ;

# 807 "/usr/include/stdlib.h" 3 4
extern char *ecvt (double __value, int __ndigit, int *__restrict __decpt,
     int *__restrict __sign) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4))) ;




extern char *fcvt (double __value, int __ndigit, int *__restrict __decpt,
     int *__restrict __sign) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4))) ;




extern char *gcvt (double __value, int __ndigit, char *__buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3))) ;




extern char *qecvt (long double __value, int __ndigit,
      int *__restrict __decpt, int *__restrict __sign)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4))) ;
extern char *qfcvt (long double __value, int __ndigit,
      int *__restrict __decpt, int *__restrict __sign)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4))) ;
extern char *qgcvt (long double __value, int __ndigit, char *__buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3))) ;




extern int ecvt_r (double __value, int __ndigit, int *__restrict __decpt,
     int *__restrict __sign, char *__restrict __buf,
     size_t __len) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4, 5)));
extern int fcvt_r (double __value, int __ndigit, int *__restrict __decpt,
     int *__restrict __sign, char *__restrict __buf,
     size_t __len) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4, 5)));

extern int qecvt_r (long double __value, int __ndigit,
      int *__restrict __decpt, int *__restrict __sign,
      char *__restrict __buf, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4, 5)));
extern int qfcvt_r (long double __value, int __ndigit,
      int *__restrict __decpt, int *__restrict __sign,
      char *__restrict __buf, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (3, 4, 5)));







extern int mblen (const char *__s, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) ;


extern int mbtowc (wchar_t *__restrict __pwc,
     const char *__restrict __s, size_t __n) __attribute__ ((__nothrow__ , __leaf__)) ;


extern int wctomb (char *__s, wchar_t __wchar) __attribute__ ((__nothrow__ , __leaf__)) ;



extern size_t mbstowcs (wchar_t *__restrict __pwcs,
   const char *__restrict __s, size_t __n) __attribute__ ((__nothrow__ , __leaf__));

extern size_t wcstombs (char *__restrict __s,
   const wchar_t *__restrict __pwcs, size_t __n)
     __attribute__ ((__nothrow__ , __leaf__));








extern int rpmatch (const char *__response) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
# 895 "/usr/include/stdlib.h" 3 4
extern int getsubopt (char **__restrict __optionp,
        char *const *__restrict __tokens,
        char **__restrict __valuep)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2, 3))) ;





extern void setkey (const char *__key) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));







extern int posix_openpt (int __oflag) ;







extern int grantpt (int __fd) __attribute__ ((__nothrow__ , __leaf__));



extern int unlockpt (int __fd) __attribute__ ((__nothrow__ , __leaf__));




extern char *ptsname (int __fd) __attribute__ ((__nothrow__ , __leaf__)) ;






extern int ptsname_r (int __fd, char *__buf, size_t __buflen)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));


extern int getpt (void);






extern int getloadavg (double __loadavg[], int __nelem)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


# 1 "/usr/include/bits/stdlib-float.h" 1 3 4
# 952 "/usr/include/stdlib.h" 2 3 4
# 964 "/usr/include/stdlib.h" 3 4

# 11 "db2_utils.c" 2

# 1 "/usr/include/unistd.h" 1 3 4
# 27 "/usr/include/unistd.h" 3 4

# 202 "/usr/include/unistd.h" 3 4
# 1 "/usr/include/bits/posix_opt.h" 1 3 4
# 203 "/usr/include/unistd.h" 2 3 4



# 1 "/usr/include/bits/environments.h" 1 3 4
# 22 "/usr/include/bits/environments.h" 3 4
# 1 "/usr/include/bits/wordsize.h" 1 3 4
# 23 "/usr/include/bits/environments.h" 2 3 4
# 207 "/usr/include/unistd.h" 2 3 4
# 226 "/usr/include/unistd.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 227 "/usr/include/unistd.h" 2 3 4
# 267 "/usr/include/unistd.h" 3 4
typedef __intptr_t intptr_t;






typedef __socklen_t socklen_t;
# 287 "/usr/include/unistd.h" 3 4
extern int access (const char *__name, int __type) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));




extern int euidaccess (const char *__name, int __type)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern int eaccess (const char *__name, int __type)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));






extern int faccessat (int __fd, const char *__file, int __type, int __flag)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2))) ;
# 334 "/usr/include/unistd.h" 3 4
extern __off_t lseek (int __fd, __off_t __offset, int __whence) __attribute__ ((__nothrow__ , __leaf__));
# 345 "/usr/include/unistd.h" 3 4
extern __off64_t lseek64 (int __fd, __off64_t __offset, int __whence)
     __attribute__ ((__nothrow__ , __leaf__));






extern int close (int __fd);






extern ssize_t read (int __fd, void *__buf, size_t __nbytes) ;





extern ssize_t write (int __fd, const void *__buf, size_t __n) ;
# 376 "/usr/include/unistd.h" 3 4
extern ssize_t pread (int __fd, void *__buf, size_t __nbytes,
        __off_t __offset) ;






extern ssize_t pwrite (int __fd, const void *__buf, size_t __n,
         __off_t __offset) ;
# 404 "/usr/include/unistd.h" 3 4
extern ssize_t pread64 (int __fd, void *__buf, size_t __nbytes,
   __off64_t __offset) ;


extern ssize_t pwrite64 (int __fd, const void *__buf, size_t __n,
    __off64_t __offset) ;







extern int pipe (int __pipedes[2]) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int pipe2 (int __pipedes[2], int __flags) __attribute__ ((__nothrow__ , __leaf__)) ;
# 432 "/usr/include/unistd.h" 3 4
extern unsigned int alarm (unsigned int __seconds) __attribute__ ((__nothrow__ , __leaf__));
# 444 "/usr/include/unistd.h" 3 4
extern unsigned int sleep (unsigned int __seconds);







extern __useconds_t ualarm (__useconds_t __value, __useconds_t __interval)
     __attribute__ ((__nothrow__ , __leaf__));






extern int usleep (__useconds_t __useconds);
# 469 "/usr/include/unistd.h" 3 4
extern int pause (void);



extern int chown (const char *__file, __uid_t __owner, __gid_t __group)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;



extern int fchown (int __fd, __uid_t __owner, __gid_t __group) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int lchown (const char *__file, __uid_t __owner, __gid_t __group)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;






extern int fchownat (int __fd, const char *__file, __uid_t __owner,
       __gid_t __group, int __flag)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2))) ;



extern int chdir (const char *__path) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;



extern int fchdir (int __fd) __attribute__ ((__nothrow__ , __leaf__)) ;
# 511 "/usr/include/unistd.h" 3 4
extern char *getcwd (char *__buf, size_t __size) __attribute__ ((__nothrow__ , __leaf__)) ;





extern char *get_current_dir_name (void) __attribute__ ((__nothrow__ , __leaf__));







extern char *getwd (char *__buf)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) __attribute__ ((__deprecated__)) ;




extern int dup (int __fd) __attribute__ ((__nothrow__ , __leaf__)) ;


extern int dup2 (int __fd, int __fd2) __attribute__ ((__nothrow__ , __leaf__));




extern int dup3 (int __fd, int __fd2, int __flags) __attribute__ ((__nothrow__ , __leaf__));



extern char **__environ;

extern char **environ;





extern int execve (const char *__path, char *const __argv[],
     char *const __envp[]) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));




extern int fexecve (int __fd, char *const __argv[], char *const __envp[])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));




extern int execv (const char *__path, char *const __argv[])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));



extern int execle (const char *__path, const char *__arg, ...)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));



extern int execl (const char *__path, const char *__arg, ...)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));



extern int execvp (const char *__file, char *const __argv[])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));




extern int execlp (const char *__file, const char *__arg, ...)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));




extern int execvpe (const char *__file, char *const __argv[],
      char *const __envp[])
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));





extern int nice (int __inc) __attribute__ ((__nothrow__ , __leaf__)) ;




extern void _exit (int __status) __attribute__ ((__noreturn__));





# 1 "/usr/include/bits/confname.h" 1 3 4
# 25 "/usr/include/bits/confname.h" 3 4
enum
  {
    _PC_LINK_MAX,

    _PC_MAX_CANON,

    _PC_MAX_INPUT,

    _PC_NAME_MAX,

    _PC_PATH_MAX,

    _PC_PIPE_BUF,

    _PC_CHOWN_RESTRICTED,

    _PC_NO_TRUNC,

    _PC_VDISABLE,

    _PC_SYNC_IO,

    _PC_ASYNC_IO,

    _PC_PRIO_IO,

    _PC_SOCK_MAXBUF,

    _PC_FILESIZEBITS,

    _PC_REC_INCR_XFER_SIZE,

    _PC_REC_MAX_XFER_SIZE,

    _PC_REC_MIN_XFER_SIZE,

    _PC_REC_XFER_ALIGN,

    _PC_ALLOC_SIZE_MIN,

    _PC_SYMLINK_MAX,

    _PC_2_SYMLINKS

  };


enum
  {
    _SC_ARG_MAX,

    _SC_CHILD_MAX,

    _SC_CLK_TCK,

    _SC_NGROUPS_MAX,

    _SC_OPEN_MAX,

    _SC_STREAM_MAX,

    _SC_TZNAME_MAX,

    _SC_JOB_CONTROL,

    _SC_SAVED_IDS,

    _SC_REALTIME_SIGNALS,

    _SC_PRIORITY_SCHEDULING,

    _SC_TIMERS,

    _SC_ASYNCHRONOUS_IO,

    _SC_PRIORITIZED_IO,

    _SC_SYNCHRONIZED_IO,

    _SC_FSYNC,

    _SC_MAPPED_FILES,

    _SC_MEMLOCK,

    _SC_MEMLOCK_RANGE,

    _SC_MEMORY_PROTECTION,

    _SC_MESSAGE_PASSING,

    _SC_SEMAPHORES,

    _SC_SHARED_MEMORY_OBJECTS,

    _SC_AIO_LISTIO_MAX,

    _SC_AIO_MAX,

    _SC_AIO_PRIO_DELTA_MAX,

    _SC_DELAYTIMER_MAX,

    _SC_MQ_OPEN_MAX,

    _SC_MQ_PRIO_MAX,

    _SC_VERSION,

    _SC_PAGESIZE,


    _SC_RTSIG_MAX,

    _SC_SEM_NSEMS_MAX,

    _SC_SEM_VALUE_MAX,

    _SC_SIGQUEUE_MAX,

    _SC_TIMER_MAX,




    _SC_BC_BASE_MAX,

    _SC_BC_DIM_MAX,

    _SC_BC_SCALE_MAX,

    _SC_BC_STRING_MAX,

    _SC_COLL_WEIGHTS_MAX,

    _SC_EQUIV_CLASS_MAX,

    _SC_EXPR_NEST_MAX,

    _SC_LINE_MAX,

    _SC_RE_DUP_MAX,

    _SC_CHARCLASS_NAME_MAX,


    _SC_2_VERSION,

    _SC_2_C_BIND,

    _SC_2_C_DEV,

    _SC_2_FORT_DEV,

    _SC_2_FORT_RUN,

    _SC_2_SW_DEV,

    _SC_2_LOCALEDEF,


    _SC_PII,

    _SC_PII_XTI,

    _SC_PII_SOCKET,

    _SC_PII_INTERNET,

    _SC_PII_OSI,

    _SC_POLL,

    _SC_SELECT,

    _SC_UIO_MAXIOV,

    _SC_IOV_MAX = _SC_UIO_MAXIOV,

    _SC_PII_INTERNET_STREAM,

    _SC_PII_INTERNET_DGRAM,

    _SC_PII_OSI_COTS,

    _SC_PII_OSI_CLTS,

    _SC_PII_OSI_M,

    _SC_T_IOV_MAX,



    _SC_THREADS,

    _SC_THREAD_SAFE_FUNCTIONS,

    _SC_GETGR_R_SIZE_MAX,

    _SC_GETPW_R_SIZE_MAX,

    _SC_LOGIN_NAME_MAX,

    _SC_TTY_NAME_MAX,

    _SC_THREAD_DESTRUCTOR_ITERATIONS,

    _SC_THREAD_KEYS_MAX,

    _SC_THREAD_STACK_MIN,

    _SC_THREAD_THREADS_MAX,

    _SC_THREAD_ATTR_STACKADDR,

    _SC_THREAD_ATTR_STACKSIZE,

    _SC_THREAD_PRIORITY_SCHEDULING,

    _SC_THREAD_PRIO_INHERIT,

    _SC_THREAD_PRIO_PROTECT,

    _SC_THREAD_PROCESS_SHARED,


    _SC_NPROCESSORS_CONF,

    _SC_NPROCESSORS_ONLN,

    _SC_PHYS_PAGES,

    _SC_AVPHYS_PAGES,

    _SC_ATEXIT_MAX,

    _SC_PASS_MAX,


    _SC_XOPEN_VERSION,

    _SC_XOPEN_XCU_VERSION,

    _SC_XOPEN_UNIX,

    _SC_XOPEN_CRYPT,

    _SC_XOPEN_ENH_I18N,

    _SC_XOPEN_SHM,


    _SC_2_CHAR_TERM,

    _SC_2_C_VERSION,

    _SC_2_UPE,


    _SC_XOPEN_XPG2,

    _SC_XOPEN_XPG3,

    _SC_XOPEN_XPG4,


    _SC_CHAR_BIT,

    _SC_CHAR_MAX,

    _SC_CHAR_MIN,

    _SC_INT_MAX,

    _SC_INT_MIN,

    _SC_LONG_BIT,

    _SC_WORD_BIT,

    _SC_MB_LEN_MAX,

    _SC_NZERO,

    _SC_SSIZE_MAX,

    _SC_SCHAR_MAX,

    _SC_SCHAR_MIN,

    _SC_SHRT_MAX,

    _SC_SHRT_MIN,

    _SC_UCHAR_MAX,

    _SC_UINT_MAX,

    _SC_ULONG_MAX,

    _SC_USHRT_MAX,


    _SC_NL_ARGMAX,

    _SC_NL_LANGMAX,

    _SC_NL_MSGMAX,

    _SC_NL_NMAX,

    _SC_NL_SETMAX,

    _SC_NL_TEXTMAX,


    _SC_XBS5_ILP32_OFF32,

    _SC_XBS5_ILP32_OFFBIG,

    _SC_XBS5_LP64_OFF64,

    _SC_XBS5_LPBIG_OFFBIG,


    _SC_XOPEN_LEGACY,

    _SC_XOPEN_REALTIME,

    _SC_XOPEN_REALTIME_THREADS,


    _SC_ADVISORY_INFO,

    _SC_BARRIERS,

    _SC_BASE,

    _SC_C_LANG_SUPPORT,

    _SC_C_LANG_SUPPORT_R,

    _SC_CLOCK_SELECTION,

    _SC_CPUTIME,

    _SC_THREAD_CPUTIME,

    _SC_DEVICE_IO,

    _SC_DEVICE_SPECIFIC,

    _SC_DEVICE_SPECIFIC_R,

    _SC_FD_MGMT,

    _SC_FIFO,

    _SC_PIPE,

    _SC_FILE_ATTRIBUTES,

    _SC_FILE_LOCKING,

    _SC_FILE_SYSTEM,

    _SC_MONOTONIC_CLOCK,

    _SC_MULTI_PROCESS,

    _SC_SINGLE_PROCESS,

    _SC_NETWORKING,

    _SC_READER_WRITER_LOCKS,

    _SC_SPIN_LOCKS,

    _SC_REGEXP,

    _SC_REGEX_VERSION,

    _SC_SHELL,

    _SC_SIGNALS,

    _SC_SPAWN,

    _SC_SPORADIC_SERVER,

    _SC_THREAD_SPORADIC_SERVER,

    _SC_SYSTEM_DATABASE,

    _SC_SYSTEM_DATABASE_R,

    _SC_TIMEOUTS,

    _SC_TYPED_MEMORY_OBJECTS,

    _SC_USER_GROUPS,

    _SC_USER_GROUPS_R,

    _SC_2_PBS,

    _SC_2_PBS_ACCOUNTING,

    _SC_2_PBS_LOCATE,

    _SC_2_PBS_MESSAGE,

    _SC_2_PBS_TRACK,

    _SC_SYMLOOP_MAX,

    _SC_STREAMS,

    _SC_2_PBS_CHECKPOINT,


    _SC_V6_ILP32_OFF32,

    _SC_V6_ILP32_OFFBIG,

    _SC_V6_LP64_OFF64,

    _SC_V6_LPBIG_OFFBIG,


    _SC_HOST_NAME_MAX,

    _SC_TRACE,

    _SC_TRACE_EVENT_FILTER,

    _SC_TRACE_INHERIT,

    _SC_TRACE_LOG,


    _SC_LEVEL1_ICACHE_SIZE,

    _SC_LEVEL1_ICACHE_ASSOC,

    _SC_LEVEL1_ICACHE_LINESIZE,

    _SC_LEVEL1_DCACHE_SIZE,

    _SC_LEVEL1_DCACHE_ASSOC,

    _SC_LEVEL1_DCACHE_LINESIZE,

    _SC_LEVEL2_CACHE_SIZE,

    _SC_LEVEL2_CACHE_ASSOC,

    _SC_LEVEL2_CACHE_LINESIZE,

    _SC_LEVEL3_CACHE_SIZE,

    _SC_LEVEL3_CACHE_ASSOC,

    _SC_LEVEL3_CACHE_LINESIZE,

    _SC_LEVEL4_CACHE_SIZE,

    _SC_LEVEL4_CACHE_ASSOC,

    _SC_LEVEL4_CACHE_LINESIZE,



    _SC_IPV6 = _SC_LEVEL1_ICACHE_SIZE + 50,

    _SC_RAW_SOCKETS,


    _SC_V7_ILP32_OFF32,

    _SC_V7_ILP32_OFFBIG,

    _SC_V7_LP64_OFF64,

    _SC_V7_LPBIG_OFFBIG,


    _SC_SS_REPL_MAX,


    _SC_TRACE_EVENT_NAME_MAX,

    _SC_TRACE_NAME_MAX,

    _SC_TRACE_SYS_MAX,

    _SC_TRACE_USER_EVENT_MAX,


    _SC_XOPEN_STREAMS,


    _SC_THREAD_ROBUST_PRIO_INHERIT,

    _SC_THREAD_ROBUST_PRIO_PROTECT

  };


enum
  {
    _CS_PATH,


    _CS_V6_WIDTH_RESTRICTED_ENVS,



    _CS_GNU_LIBC_VERSION,

    _CS_GNU_LIBPTHREAD_VERSION,


    _CS_V5_WIDTH_RESTRICTED_ENVS,



    _CS_V7_WIDTH_RESTRICTED_ENVS,



    _CS_LFS_CFLAGS = 1000,

    _CS_LFS_LDFLAGS,

    _CS_LFS_LIBS,

    _CS_LFS_LINTFLAGS,

    _CS_LFS64_CFLAGS,

    _CS_LFS64_LDFLAGS,

    _CS_LFS64_LIBS,

    _CS_LFS64_LINTFLAGS,


    _CS_XBS5_ILP32_OFF32_CFLAGS = 1100,

    _CS_XBS5_ILP32_OFF32_LDFLAGS,

    _CS_XBS5_ILP32_OFF32_LIBS,

    _CS_XBS5_ILP32_OFF32_LINTFLAGS,

    _CS_XBS5_ILP32_OFFBIG_CFLAGS,

    _CS_XBS5_ILP32_OFFBIG_LDFLAGS,

    _CS_XBS5_ILP32_OFFBIG_LIBS,

    _CS_XBS5_ILP32_OFFBIG_LINTFLAGS,

    _CS_XBS5_LP64_OFF64_CFLAGS,

    _CS_XBS5_LP64_OFF64_LDFLAGS,

    _CS_XBS5_LP64_OFF64_LIBS,

    _CS_XBS5_LP64_OFF64_LINTFLAGS,

    _CS_XBS5_LPBIG_OFFBIG_CFLAGS,

    _CS_XBS5_LPBIG_OFFBIG_LDFLAGS,

    _CS_XBS5_LPBIG_OFFBIG_LIBS,

    _CS_XBS5_LPBIG_OFFBIG_LINTFLAGS,


    _CS_POSIX_V6_ILP32_OFF32_CFLAGS,

    _CS_POSIX_V6_ILP32_OFF32_LDFLAGS,

    _CS_POSIX_V6_ILP32_OFF32_LIBS,

    _CS_POSIX_V6_ILP32_OFF32_LINTFLAGS,

    _CS_POSIX_V6_ILP32_OFFBIG_CFLAGS,

    _CS_POSIX_V6_ILP32_OFFBIG_LDFLAGS,

    _CS_POSIX_V6_ILP32_OFFBIG_LIBS,

    _CS_POSIX_V6_ILP32_OFFBIG_LINTFLAGS,

    _CS_POSIX_V6_LP64_OFF64_CFLAGS,

    _CS_POSIX_V6_LP64_OFF64_LDFLAGS,

    _CS_POSIX_V6_LP64_OFF64_LIBS,

    _CS_POSIX_V6_LP64_OFF64_LINTFLAGS,

    _CS_POSIX_V6_LPBIG_OFFBIG_CFLAGS,

    _CS_POSIX_V6_LPBIG_OFFBIG_LDFLAGS,

    _CS_POSIX_V6_LPBIG_OFFBIG_LIBS,

    _CS_POSIX_V6_LPBIG_OFFBIG_LINTFLAGS,


    _CS_POSIX_V7_ILP32_OFF32_CFLAGS,

    _CS_POSIX_V7_ILP32_OFF32_LDFLAGS,

    _CS_POSIX_V7_ILP32_OFF32_LIBS,

    _CS_POSIX_V7_ILP32_OFF32_LINTFLAGS,

    _CS_POSIX_V7_ILP32_OFFBIG_CFLAGS,

    _CS_POSIX_V7_ILP32_OFFBIG_LDFLAGS,

    _CS_POSIX_V7_ILP32_OFFBIG_LIBS,

    _CS_POSIX_V7_ILP32_OFFBIG_LINTFLAGS,

    _CS_POSIX_V7_LP64_OFF64_CFLAGS,

    _CS_POSIX_V7_LP64_OFF64_LDFLAGS,

    _CS_POSIX_V7_LP64_OFF64_LIBS,

    _CS_POSIX_V7_LP64_OFF64_LINTFLAGS,

    _CS_POSIX_V7_LPBIG_OFFBIG_CFLAGS,

    _CS_POSIX_V7_LPBIG_OFFBIG_LDFLAGS,

    _CS_POSIX_V7_LPBIG_OFFBIG_LIBS,

    _CS_POSIX_V7_LPBIG_OFFBIG_LINTFLAGS,


    _CS_V6_ENV,

    _CS_V7_ENV

  };
# 610 "/usr/include/unistd.h" 2 3 4


extern long int pathconf (const char *__path, int __name)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));


extern long int fpathconf (int __fd, int __name) __attribute__ ((__nothrow__ , __leaf__));


extern long int sysconf (int __name) __attribute__ ((__nothrow__ , __leaf__));



extern size_t confstr (int __name, char *__buf, size_t __len) __attribute__ ((__nothrow__ , __leaf__));




extern __pid_t getpid (void) __attribute__ ((__nothrow__ , __leaf__));


extern __pid_t getppid (void) __attribute__ ((__nothrow__ , __leaf__));




extern __pid_t getpgrp (void) __attribute__ ((__nothrow__ , __leaf__));
# 646 "/usr/include/unistd.h" 3 4
extern __pid_t __getpgid (__pid_t __pid) __attribute__ ((__nothrow__ , __leaf__));

extern __pid_t getpgid (__pid_t __pid) __attribute__ ((__nothrow__ , __leaf__));






extern int setpgid (__pid_t __pid, __pid_t __pgid) __attribute__ ((__nothrow__ , __leaf__));
# 672 "/usr/include/unistd.h" 3 4
extern int setpgrp (void) __attribute__ ((__nothrow__ , __leaf__));
# 689 "/usr/include/unistd.h" 3 4
extern __pid_t setsid (void) __attribute__ ((__nothrow__ , __leaf__));



extern __pid_t getsid (__pid_t __pid) __attribute__ ((__nothrow__ , __leaf__));



extern __uid_t getuid (void) __attribute__ ((__nothrow__ , __leaf__));


extern __uid_t geteuid (void) __attribute__ ((__nothrow__ , __leaf__));


extern __gid_t getgid (void) __attribute__ ((__nothrow__ , __leaf__));


extern __gid_t getegid (void) __attribute__ ((__nothrow__ , __leaf__));




extern int getgroups (int __size, __gid_t __list[]) __attribute__ ((__nothrow__ , __leaf__)) ;



extern int group_member (__gid_t __gid) __attribute__ ((__nothrow__ , __leaf__));






extern int setuid (__uid_t __uid) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int setreuid (__uid_t __ruid, __uid_t __euid) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int seteuid (__uid_t __uid) __attribute__ ((__nothrow__ , __leaf__)) ;






extern int setgid (__gid_t __gid) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int setregid (__gid_t __rgid, __gid_t __egid) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int setegid (__gid_t __gid) __attribute__ ((__nothrow__ , __leaf__)) ;





extern int getresuid (__uid_t *__ruid, __uid_t *__euid, __uid_t *__suid)
     __attribute__ ((__nothrow__ , __leaf__));



extern int getresgid (__gid_t *__rgid, __gid_t *__egid, __gid_t *__sgid)
     __attribute__ ((__nothrow__ , __leaf__));



extern int setresuid (__uid_t __ruid, __uid_t __euid, __uid_t __suid)
     __attribute__ ((__nothrow__ , __leaf__)) ;



extern int setresgid (__gid_t __rgid, __gid_t __egid, __gid_t __sgid)
     __attribute__ ((__nothrow__ , __leaf__)) ;






extern __pid_t fork (void) __attribute__ ((__nothrow__));







extern __pid_t vfork (void) __attribute__ ((__nothrow__ , __leaf__));





extern char *ttyname (int __fd) __attribute__ ((__nothrow__ , __leaf__));



extern int ttyname_r (int __fd, char *__buf, size_t __buflen)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2))) ;



extern int isatty (int __fd) __attribute__ ((__nothrow__ , __leaf__));





extern int ttyslot (void) __attribute__ ((__nothrow__ , __leaf__));




extern int link (const char *__from, const char *__to)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2))) ;




extern int linkat (int __fromfd, const char *__from, int __tofd,
     const char *__to, int __flags)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 4))) ;




extern int symlink (const char *__from, const char *__to)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2))) ;




extern ssize_t readlink (const char *__restrict __path,
    char *__restrict __buf, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2))) ;




extern int symlinkat (const char *__from, int __tofd,
        const char *__to) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 3))) ;


extern ssize_t readlinkat (int __fd, const char *__restrict __path,
      char *__restrict __buf, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2, 3))) ;



extern int unlink (const char *__name) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));



extern int unlinkat (int __fd, const char *__name, int __flag)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (2)));



extern int rmdir (const char *__path) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));



extern __pid_t tcgetpgrp (int __fd) __attribute__ ((__nothrow__ , __leaf__));


extern int tcsetpgrp (int __fd, __pid_t __pgrp_id) __attribute__ ((__nothrow__ , __leaf__));






extern char *getlogin (void);







extern int getlogin_r (char *__name, size_t __name_len) __attribute__ ((__nonnull__ (1)));




extern int setlogin (const char *__name) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));
# 893 "/usr/include/unistd.h" 3 4
# 1 "/usr/include/getopt.h" 1 3 4
# 58 "/usr/include/getopt.h" 3 4
extern char *optarg;
# 72 "/usr/include/getopt.h" 3 4
extern int optind;




extern int opterr;



extern int optopt;
# 151 "/usr/include/getopt.h" 3 4
extern int getopt (int ___argc, char *const *___argv, const char *__shortopts)
       __attribute__ ((__nothrow__ , __leaf__));
# 894 "/usr/include/unistd.h" 2 3 4







extern int gethostname (char *__name, size_t __len) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));






extern int sethostname (const char *__name, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;



extern int sethostid (long int __id) __attribute__ ((__nothrow__ , __leaf__)) ;





extern int getdomainname (char *__name, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
extern int setdomainname (const char *__name, size_t __len)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;





extern int vhangup (void) __attribute__ ((__nothrow__ , __leaf__));


extern int revoke (const char *__file) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;







extern int profil (unsigned short int *__sample_buffer, size_t __size,
     size_t __offset, unsigned int __scale)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));





extern int acct (const char *__name) __attribute__ ((__nothrow__ , __leaf__));



extern char *getusershell (void) __attribute__ ((__nothrow__ , __leaf__));
extern void endusershell (void) __attribute__ ((__nothrow__ , __leaf__));
extern void setusershell (void) __attribute__ ((__nothrow__ , __leaf__));





extern int daemon (int __nochdir, int __noclose) __attribute__ ((__nothrow__ , __leaf__)) ;






extern int chroot (const char *__path) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;



extern char *getpass (const char *__prompt) __attribute__ ((__nonnull__ (1)));







extern int fsync (int __fd);





extern int syncfs (int __fd) __attribute__ ((__nothrow__ , __leaf__));






extern long int gethostid (void);


extern void sync (void) __attribute__ ((__nothrow__ , __leaf__));





extern int getpagesize (void) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__const__));




extern int getdtablesize (void) __attribute__ ((__nothrow__ , __leaf__));
# 1015 "/usr/include/unistd.h" 3 4
extern int truncate (const char *__file, __off_t __length)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
# 1027 "/usr/include/unistd.h" 3 4
extern int truncate64 (const char *__file, __off64_t __length)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1))) ;
# 1038 "/usr/include/unistd.h" 3 4
extern int ftruncate (int __fd, __off_t __length) __attribute__ ((__nothrow__ , __leaf__)) ;
# 1048 "/usr/include/unistd.h" 3 4
extern int ftruncate64 (int __fd, __off64_t __length) __attribute__ ((__nothrow__ , __leaf__)) ;
# 1059 "/usr/include/unistd.h" 3 4
extern int brk (void *__addr) __attribute__ ((__nothrow__ , __leaf__)) ;





extern void *sbrk (intptr_t __delta) __attribute__ ((__nothrow__ , __leaf__));
# 1080 "/usr/include/unistd.h" 3 4
extern long int syscall (long int __sysno, ...) __attribute__ ((__nothrow__ , __leaf__));
# 1103 "/usr/include/unistd.h" 3 4
extern int lockf (int __fd, int __cmd, __off_t __len) ;
# 1113 "/usr/include/unistd.h" 3 4
extern int lockf64 (int __fd, int __cmd, __off64_t __len) ;
# 1134 "/usr/include/unistd.h" 3 4
extern int fdatasync (int __fildes);







extern char *crypt (const char *__key, const char *__salt)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));



extern void encrypt (char *__block, int __edflag) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1)));






extern void swab (const void *__restrict __from, void *__restrict __to,
    ssize_t __n) __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__nonnull__ (1, 2)));
# 1172 "/usr/include/unistd.h" 3 4

# 13 "db2_utils.c" 2
# 1 "/usr/include/stdio.h" 1 3 4
# 29 "/usr/include/stdio.h" 3 4




# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 34 "/usr/include/stdio.h" 2 3 4
# 44 "/usr/include/stdio.h" 3 4
struct _IO_FILE;



typedef struct _IO_FILE FILE;





# 64 "/usr/include/stdio.h" 3 4
typedef struct _IO_FILE __FILE;
# 74 "/usr/include/stdio.h" 3 4
# 1 "/usr/include/libio.h" 1 3 4
# 32 "/usr/include/libio.h" 3 4
# 1 "/usr/include/_G_config.h" 1 3 4
# 15 "/usr/include/_G_config.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 16 "/usr/include/_G_config.h" 2 3 4




# 1 "/usr/include/wchar.h" 1 3 4
# 82 "/usr/include/wchar.h" 3 4
typedef struct
{
  int __count;
  union
  {

    unsigned int __wch;



    char __wchb[4];
  } __value;
} __mbstate_t;
# 21 "/usr/include/_G_config.h" 2 3 4
typedef struct
{
  __off_t __pos;
  __mbstate_t __state;
} _G_fpos_t;
typedef struct
{
  __off64_t __pos;
  __mbstate_t __state;
} _G_fpos64_t;
# 33 "/usr/include/libio.h" 2 3 4
# 50 "/usr/include/libio.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stdarg.h" 1 3 4
# 40 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stdarg.h" 3 4
typedef __builtin_va_list __gnuc_va_list;
# 51 "/usr/include/libio.h" 2 3 4
# 145 "/usr/include/libio.h" 3 4
struct _IO_jump_t; struct _IO_FILE;
# 155 "/usr/include/libio.h" 3 4
typedef void _IO_lock_t;





struct _IO_marker {
  struct _IO_marker *_next;
  struct _IO_FILE *_sbuf;



  int _pos;
# 178 "/usr/include/libio.h" 3 4
};


enum __codecvt_result
{
  __codecvt_ok,
  __codecvt_partial,
  __codecvt_error,
  __codecvt_noconv
};
# 246 "/usr/include/libio.h" 3 4
struct _IO_FILE {
  int _flags;




  char* _IO_read_ptr;
  char* _IO_read_end;
  char* _IO_read_base;
  char* _IO_write_base;
  char* _IO_write_ptr;
  char* _IO_write_end;
  char* _IO_buf_base;
  char* _IO_buf_end;

  char *_IO_save_base;
  char *_IO_backup_base;
  char *_IO_save_end;

  struct _IO_marker *_markers;

  struct _IO_FILE *_chain;

  int _fileno;



  int _flags2;

  __off_t _old_offset;



  unsigned short _cur_column;
  signed char _vtable_offset;
  char _shortbuf[1];



  _IO_lock_t *_lock;
# 294 "/usr/include/libio.h" 3 4
  __off64_t _offset;
# 303 "/usr/include/libio.h" 3 4
  void *__pad1;
  void *__pad2;
  void *__pad3;
  void *__pad4;
  size_t __pad5;

  int _mode;

  char _unused2[15 * sizeof (int) - 4 * sizeof (void *) - sizeof (size_t)];

};


typedef struct _IO_FILE _IO_FILE;


struct _IO_FILE_plus;

extern struct _IO_FILE_plus _IO_2_1_stdin_;
extern struct _IO_FILE_plus _IO_2_1_stdout_;
extern struct _IO_FILE_plus _IO_2_1_stderr_;
# 339 "/usr/include/libio.h" 3 4
typedef __ssize_t __io_read_fn (void *__cookie, char *__buf, size_t __nbytes);







typedef __ssize_t __io_write_fn (void *__cookie, const char *__buf,
     size_t __n);







typedef int __io_seek_fn (void *__cookie, __off64_t *__pos, int __w);


typedef int __io_close_fn (void *__cookie);




typedef __io_read_fn cookie_read_function_t;
typedef __io_write_fn cookie_write_function_t;
typedef __io_seek_fn cookie_seek_function_t;
typedef __io_close_fn cookie_close_function_t;


typedef struct
{
  __io_read_fn *read;
  __io_write_fn *write;
  __io_seek_fn *seek;
  __io_close_fn *close;
} _IO_cookie_io_functions_t;
typedef _IO_cookie_io_functions_t cookie_io_functions_t;

struct _IO_cookie_file;


extern void _IO_cookie_init (struct _IO_cookie_file *__cfile, int __read_write,
        void *__cookie, _IO_cookie_io_functions_t __fns);







extern int __underflow (_IO_FILE *);
extern int __uflow (_IO_FILE *);
extern int __overflow (_IO_FILE *, int);
# 435 "/usr/include/libio.h" 3 4
extern int _IO_getc (_IO_FILE *__fp);
extern int _IO_putc (int __c, _IO_FILE *__fp);
extern int _IO_feof (_IO_FILE *__fp) __attribute__ ((__nothrow__ , __leaf__));
extern int _IO_ferror (_IO_FILE *__fp) __attribute__ ((__nothrow__ , __leaf__));

extern int _IO_peekc_locked (_IO_FILE *__fp);





extern void _IO_flockfile (_IO_FILE *) __attribute__ ((__nothrow__ , __leaf__));
extern void _IO_funlockfile (_IO_FILE *) __attribute__ ((__nothrow__ , __leaf__));
extern int _IO_ftrylockfile (_IO_FILE *) __attribute__ ((__nothrow__ , __leaf__));
# 465 "/usr/include/libio.h" 3 4
extern int _IO_vfscanf (_IO_FILE * __restrict, const char * __restrict,
   __gnuc_va_list, int *__restrict);
extern int _IO_vfprintf (_IO_FILE *__restrict, const char *__restrict,
    __gnuc_va_list);
extern __ssize_t _IO_padn (_IO_FILE *, int, __ssize_t);
extern size_t _IO_sgetn (_IO_FILE *, void *, size_t);

extern __off64_t _IO_seekoff (_IO_FILE *, __off64_t, int, int);
extern __off64_t _IO_seekpos (_IO_FILE *, __off64_t, int);

extern void _IO_free_backup_area (_IO_FILE *) __attribute__ ((__nothrow__ , __leaf__));
# 75 "/usr/include/stdio.h" 2 3 4




typedef __gnuc_va_list va_list;
# 108 "/usr/include/stdio.h" 3 4


typedef _G_fpos_t fpos_t;





typedef _G_fpos64_t fpos64_t;
# 164 "/usr/include/stdio.h" 3 4
# 1 "/usr/include/bits/stdio_lim.h" 1 3 4
# 165 "/usr/include/stdio.h" 2 3 4



extern struct _IO_FILE *stdin;
extern struct _IO_FILE *stdout;
extern struct _IO_FILE *stderr;







extern int remove (const char *__filename) __attribute__ ((__nothrow__ , __leaf__));

extern int rename (const char *__old, const char *__new) __attribute__ ((__nothrow__ , __leaf__));




extern int renameat (int __oldfd, const char *__old, int __newfd,
       const char *__new) __attribute__ ((__nothrow__ , __leaf__));








extern FILE *tmpfile (void) ;
# 205 "/usr/include/stdio.h" 3 4
extern FILE *tmpfile64 (void) ;



extern char *tmpnam (char *__s) __attribute__ ((__nothrow__ , __leaf__)) ;





extern char *tmpnam_r (char *__s) __attribute__ ((__nothrow__ , __leaf__)) ;
# 227 "/usr/include/stdio.h" 3 4
extern char *tempnam (const char *__dir, const char *__pfx)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__malloc__)) ;








extern int fclose (FILE *__stream);




extern int fflush (FILE *__stream);

# 252 "/usr/include/stdio.h" 3 4
extern int fflush_unlocked (FILE *__stream);
# 262 "/usr/include/stdio.h" 3 4
extern int fcloseall (void);









extern FILE *fopen (const char *__restrict __filename,
      const char *__restrict __modes) ;




extern FILE *freopen (const char *__restrict __filename,
        const char *__restrict __modes,
        FILE *__restrict __stream) ;
# 295 "/usr/include/stdio.h" 3 4


extern FILE *fopen64 (const char *__restrict __filename,
        const char *__restrict __modes) ;
extern FILE *freopen64 (const char *__restrict __filename,
   const char *__restrict __modes,
   FILE *__restrict __stream) ;




extern FILE *fdopen (int __fd, const char *__modes) __attribute__ ((__nothrow__ , __leaf__)) ;





extern FILE *fopencookie (void *__restrict __magic_cookie,
     const char *__restrict __modes,
     _IO_cookie_io_functions_t __io_funcs) __attribute__ ((__nothrow__ , __leaf__)) ;




extern FILE *fmemopen (void *__s, size_t __len, const char *__modes)
  __attribute__ ((__nothrow__ , __leaf__)) ;




extern FILE *open_memstream (char **__bufloc, size_t *__sizeloc) __attribute__ ((__nothrow__ , __leaf__)) ;






extern void setbuf (FILE *__restrict __stream, char *__restrict __buf) __attribute__ ((__nothrow__ , __leaf__));



extern int setvbuf (FILE *__restrict __stream, char *__restrict __buf,
      int __modes, size_t __n) __attribute__ ((__nothrow__ , __leaf__));





extern void setbuffer (FILE *__restrict __stream, char *__restrict __buf,
         size_t __size) __attribute__ ((__nothrow__ , __leaf__));


extern void setlinebuf (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__));








extern int fprintf (FILE *__restrict __stream,
      const char *__restrict __format, ...);




extern int printf (const char *__restrict __format, ...);

extern int sprintf (char *__restrict __s,
      const char *__restrict __format, ...) __attribute__ ((__nothrow__));





extern int vfprintf (FILE *__restrict __s, const char *__restrict __format,
       __gnuc_va_list __arg);




extern int vprintf (const char *__restrict __format, __gnuc_va_list __arg);

extern int vsprintf (char *__restrict __s, const char *__restrict __format,
       __gnuc_va_list __arg) __attribute__ ((__nothrow__));





extern int snprintf (char *__restrict __s, size_t __maxlen,
       const char *__restrict __format, ...)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 3, 4)));

extern int vsnprintf (char *__restrict __s, size_t __maxlen,
        const char *__restrict __format, __gnuc_va_list __arg)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 3, 0)));






extern int vasprintf (char **__restrict __ptr, const char *__restrict __f,
        __gnuc_va_list __arg)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 2, 0))) ;
extern int __asprintf (char **__restrict __ptr,
         const char *__restrict __fmt, ...)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 2, 3))) ;
extern int asprintf (char **__restrict __ptr,
       const char *__restrict __fmt, ...)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 2, 3))) ;




extern int vdprintf (int __fd, const char *__restrict __fmt,
       __gnuc_va_list __arg)
     __attribute__ ((__format__ (__printf__, 2, 0)));
extern int dprintf (int __fd, const char *__restrict __fmt, ...)
     __attribute__ ((__format__ (__printf__, 2, 3)));








extern int fscanf (FILE *__restrict __stream,
     const char *__restrict __format, ...) ;




extern int scanf (const char *__restrict __format, ...) ;

extern int sscanf (const char *__restrict __s,
     const char *__restrict __format, ...) __attribute__ ((__nothrow__ , __leaf__));
# 463 "/usr/include/stdio.h" 3 4








extern int vfscanf (FILE *__restrict __s, const char *__restrict __format,
      __gnuc_va_list __arg)
     __attribute__ ((__format__ (__scanf__, 2, 0))) ;





extern int vscanf (const char *__restrict __format, __gnuc_va_list __arg)
     __attribute__ ((__format__ (__scanf__, 1, 0))) ;


extern int vsscanf (const char *__restrict __s,
      const char *__restrict __format, __gnuc_va_list __arg)
     __attribute__ ((__nothrow__ , __leaf__)) __attribute__ ((__format__ (__scanf__, 2, 0)));
# 522 "/usr/include/stdio.h" 3 4









extern int fgetc (FILE *__stream);
extern int getc (FILE *__stream);





extern int getchar (void);

# 550 "/usr/include/stdio.h" 3 4
extern int getc_unlocked (FILE *__stream);
extern int getchar_unlocked (void);
# 561 "/usr/include/stdio.h" 3 4
extern int fgetc_unlocked (FILE *__stream);











extern int fputc (int __c, FILE *__stream);
extern int putc (int __c, FILE *__stream);





extern int putchar (int __c);

# 594 "/usr/include/stdio.h" 3 4
extern int fputc_unlocked (int __c, FILE *__stream);







extern int putc_unlocked (int __c, FILE *__stream);
extern int putchar_unlocked (int __c);






extern int getw (FILE *__stream);


extern int putw (int __w, FILE *__stream);








extern char *fgets (char *__restrict __s, int __n, FILE *__restrict __stream)
     ;
# 640 "/usr/include/stdio.h" 3 4

# 649 "/usr/include/stdio.h" 3 4
extern char *fgets_unlocked (char *__restrict __s, int __n,
        FILE *__restrict __stream) ;
# 665 "/usr/include/stdio.h" 3 4
extern __ssize_t __getdelim (char **__restrict __lineptr,
          size_t *__restrict __n, int __delimiter,
          FILE *__restrict __stream) ;
extern __ssize_t getdelim (char **__restrict __lineptr,
        size_t *__restrict __n, int __delimiter,
        FILE *__restrict __stream) ;







extern __ssize_t getline (char **__restrict __lineptr,
       size_t *__restrict __n,
       FILE *__restrict __stream) ;








extern int fputs (const char *__restrict __s, FILE *__restrict __stream);





extern int puts (const char *__s);






extern int ungetc (int __c, FILE *__stream);






extern size_t fread (void *__restrict __ptr, size_t __size,
       size_t __n, FILE *__restrict __stream) ;




extern size_t fwrite (const void *__restrict __ptr, size_t __size,
        size_t __n, FILE *__restrict __s);

# 726 "/usr/include/stdio.h" 3 4
extern int fputs_unlocked (const char *__restrict __s,
      FILE *__restrict __stream);
# 737 "/usr/include/stdio.h" 3 4
extern size_t fread_unlocked (void *__restrict __ptr, size_t __size,
         size_t __n, FILE *__restrict __stream) ;
extern size_t fwrite_unlocked (const void *__restrict __ptr, size_t __size,
          size_t __n, FILE *__restrict __stream);








extern int fseek (FILE *__stream, long int __off, int __whence);




extern long int ftell (FILE *__stream) ;




extern void rewind (FILE *__stream);

# 773 "/usr/include/stdio.h" 3 4
extern int fseeko (FILE *__stream, __off_t __off, int __whence);




extern __off_t ftello (FILE *__stream) ;
# 792 "/usr/include/stdio.h" 3 4






extern int fgetpos (FILE *__restrict __stream, fpos_t *__restrict __pos);




extern int fsetpos (FILE *__stream, const fpos_t *__pos);
# 815 "/usr/include/stdio.h" 3 4



extern int fseeko64 (FILE *__stream, __off64_t __off, int __whence);
extern __off64_t ftello64 (FILE *__stream) ;
extern int fgetpos64 (FILE *__restrict __stream, fpos64_t *__restrict __pos);
extern int fsetpos64 (FILE *__stream, const fpos64_t *__pos);




extern void clearerr (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__));

extern int feof (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;

extern int ferror (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;




extern void clearerr_unlocked (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__));
extern int feof_unlocked (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;
extern int ferror_unlocked (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;








extern void perror (const char *__s);






# 1 "/usr/include/bits/sys_errlist.h" 1 3 4
# 26 "/usr/include/bits/sys_errlist.h" 3 4
extern int sys_nerr;
extern const char *const sys_errlist[];


extern int _sys_nerr;
extern const char *const _sys_errlist[];
# 854 "/usr/include/stdio.h" 2 3 4




extern int fileno (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;




extern int fileno_unlocked (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;
# 873 "/usr/include/stdio.h" 3 4
extern FILE *popen (const char *__command, const char *__modes) ;





extern int pclose (FILE *__stream);





extern char *ctermid (char *__s) __attribute__ ((__nothrow__ , __leaf__));





extern char *cuserid (char *__s);




struct obstack;


extern int obstack_printf (struct obstack *__restrict __obstack,
      const char *__restrict __format, ...)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 2, 3)));
extern int obstack_vprintf (struct obstack *__restrict __obstack,
       const char *__restrict __format,
       __gnuc_va_list __args)
     __attribute__ ((__nothrow__)) __attribute__ ((__format__ (__printf__, 2, 0)));







extern void flockfile (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__));



extern int ftrylockfile (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__)) ;


extern void funlockfile (FILE *__stream) __attribute__ ((__nothrow__ , __leaf__));
# 943 "/usr/include/stdio.h" 3 4

# 14 "db2_utils.c" 2
# 22 "db2_utils.c"
# 1 "/home/db2inst1/sqllib/include/db2ci.h" 1
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 1 3 4
# 147 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stddef.h" 3 4
typedef long int ptrdiff_t;
# 2 "/home/db2inst1/sqllib/include/db2ci.h" 2
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 1 3 4
# 34 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 3 4
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/syslimits.h" 1 3 4






# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 1 3 4
# 168 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 3 4
# 1 "/usr/include/limits.h" 1 3 4
# 144 "/usr/include/limits.h" 3 4
# 1 "/usr/include/bits/posix1_lim.h" 1 3 4
# 160 "/usr/include/bits/posix1_lim.h" 3 4
# 1 "/usr/include/bits/local_lim.h" 1 3 4
# 38 "/usr/include/bits/local_lim.h" 3 4
# 1 "/usr/include/linux/limits.h" 1 3 4
# 39 "/usr/include/bits/local_lim.h" 2 3 4
# 161 "/usr/include/bits/posix1_lim.h" 2 3 4
# 145 "/usr/include/limits.h" 2 3 4



# 1 "/usr/include/bits/posix2_lim.h" 1 3 4
# 149 "/usr/include/limits.h" 2 3 4



# 1 "/usr/include/bits/xopen_lim.h" 1 3 4
# 33 "/usr/include/bits/xopen_lim.h" 3 4
# 1 "/usr/include/bits/stdio_lim.h" 1 3 4
# 34 "/usr/include/bits/xopen_lim.h" 2 3 4
# 153 "/usr/include/limits.h" 2 3 4
# 169 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 2 3 4
# 8 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/syslimits.h" 2 3 4
# 35 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/limits.h" 2 3 4
# 3 "/home/db2inst1/sqllib/include/db2ci.h" 2
# 1 "/usr/lib/gcc/x86_64-redhat-linux/4.8.5/include/stdarg.h" 1 3 4
# 4 "/home/db2inst1/sqllib/include/db2ci.h" 2
# 28 "/home/db2inst1/sqllib/include/db2ci.h"
typedef signed short sb2;
typedef unsigned short ub2;
typedef unsigned int ub4;
typedef signed int sb4;
typedef unsigned char ub1;
typedef signed char sb1;
typedef char eb1;
typedef short eb2;
typedef int eb4;
typedef sb1 b1;
typedef sb2 b2;
typedef sb4 b4;
typedef int eword;
typedef unsigned int uword;
typedef signed int sword;
typedef unsigned short utext;
# 70 "/home/db2inst1/sqllib/include/db2ci.h"
  typedef unsigned char oratext;
  typedef oratext OraText;






   typedef oratext text;
# 90 "/home/db2inst1/sqllib/include/db2ci.h"
 typedef unsigned long long oraub8;
 typedef signed long long orasb8;

 typedef oraub8 ub8;
 typedef orasb8 sb8;
# 121 "/home/db2inst1/sqllib/include/db2ci.h"
typedef oratext *pString;
# 147 "/home/db2inst1/sqllib/include/db2ci.h"
typedef unsigned long ubig_ora;
typedef signed long sbig_ora;
# 167 "/home/db2inst1/sqllib/include/db2ci.h"
struct xmlctx; typedef struct xmlctx xmlctx;
# 202 "/home/db2inst1/sqllib/include/db2ci.h"
typedef struct OCIAdmin OCIAdmin;
typedef struct OCIAQAgent OCIAQAgent;
typedef struct OCIAQDeqOptions OCIAQDeqOptions;
typedef struct OCIAQEnqOptions OCIAQEnqOptions;
typedef struct OCIAQLisMsgProps OCIAQLisMsgProps;
typedef struct OCIAQListenOpts OCIAQListenOpts;
typedef struct OCIAQMsgProperties OCIAQMsgProperties;
typedef struct OCIAQNfyDescriptor OCIAQNfyDescriptor;
typedef struct OCIAQSignature OCIAQSignature;
typedef struct OCIAuthInfo OCIAuthInfo;
typedef struct OCIBind OCIBind;
typedef struct OCIComplexObject OCIComplexObject;
typedef struct OCIComplexObjectComp OCIComplexObjectComp;
typedef struct OCICPool OCICPool;
typedef struct OCIDateTime OCIDateTime;
typedef struct OCIDefine OCIDefine;
typedef struct OCIDescribe OCIDescribe;
typedef struct OCIEnv OCIEnv;
typedef struct OCIError OCIError;
typedef struct OCIEvent OCIEvent;
typedef struct OCIInterval OCIInterval;
typedef struct OCILobLocator OCILobLocator;
typedef struct OCILobLocator OCIBFileLocator;
typedef struct OCILobLocator OCIBlobLocator;
typedef struct OCILobLocator OCIClobLocator;
typedef struct OCILobRegion OCILobRegion;
typedef struct OCIParam OCIParam;
typedef struct OCIResult OCIResult;
typedef struct OCIRowid OCIRowid;
typedef struct OCISecurity OCISecurity;
typedef struct OCIServer OCIServer;
typedef struct OCIServerDNs OCIServerDNs;
typedef struct OCISession OCISession;
typedef struct OCISnapshot OCISnapshot;
typedef struct OCISPool OCISPool;
typedef struct OCIStmt OCIStmt;
typedef struct OCISubscription OCISubscription;
typedef struct OCISvcCtx OCISvcCtx;
typedef struct OCITrans OCITrans;
typedef struct OCIUcb OCIUcb;
typedef ub4 OCILobLength;
typedef ub4 OCILobOffset;
typedef struct OCIColl OCIColl;
typedef OCIColl OCIArray;
typedef OCIColl OCITable;
typedef struct OCIIter OCIIter;
typedef struct OCIRaw OCIRaw;
typedef struct OCIString OCIString;
typedef ub1 nzttTStamp[4];
typedef ub4 nzttKPUsage;
typedef struct nzttIdentity nzttIdentity;
typedef struct nzttIdentityPrivate nzttIdentityPrivate;
typedef struct nzttPersona nzttPersona;
typedef struct nzttPersonaPrivate nzttPersonaPrivate;
typedef struct nzttWallet nzttWallet;
typedef struct nzttWalletPrivate nzttWalletPrivate;
typedef struct nzttWalletObj nzttWalletObj;
typedef struct nzssEntry nzssEntry;
typedef struct nzpkcs11_Info nzpkcs11_Info;

enum OCIObjectLifetime
{
   OCI_OBJECT_PERSISTENT = 1,
   OCI_OBJECT_TRANSIENT,
   OCI_OBJECT_VALUE
};
typedef enum OCIObjectLifetime OCIObjectLifetime;

typedef sb2 OCIInd;
typedef struct OCIAnyData OCIAnyData;
typedef struct OCIAnyDataCtx OCIAnyDataCtx;
typedef struct OCIAnyDataSet OCIAnyDataSet;
typedef struct OCIBinXmlReposCtx OCIBinXmlReposCtx;
typedef struct OCIDOMDocument OCIDOMDocument;
typedef struct OCIExtProcContext OCIExtProcContext;
typedef struct OCIFileObject OCIFileObject;
typedef struct OCIIOV OCIIOV;
typedef struct OCIMsg OCIMsg;
typedef struct OCIPicklerFdo OCIPicklerFdo;
typedef struct OCIPicklerImage OCIPicklerImage;
typedef struct OCIPicklerTds OCIPicklerTds;
typedef struct OCIPicklerTdsCtx OCIPicklerTdsCtx;
typedef struct OCIRef OCIRef;
typedef struct OCIThreadContext OCIThreadContext;
typedef struct OCIThreadHandle OCIThreadHandle;
typedef struct OCIThreadId OCIThreadId;
typedef struct OCIThreadKey OCIThreadKey;
typedef struct OCIThreadMutex OCIThreadMutex;
typedef struct OCIType OCIType;
typedef struct OCITypeElem OCITypeElem;
typedef struct OCITypeIter OCITypeIter;
typedef struct OCITypeMethod OCITypeMethod;
typedef struct OCIXMLType OCIXMLType;
typedef ub1 OCIObjectPropId;
typedef ub2 OCIDuration;
typedef ub4 OCIWchar;
typedef ub4 OCIPicklerTdsElement;
typedef uword OCIObjectMarkStatus;
typedef void (*OCIThreadKeyDestFunc)(void *);
typedef ub2 OCITypeCode;
typedef struct OCIDirPathColArray OCIDirPathColArray;
typedef struct OCIDirPathCtx OCIDirPathCtx;
typedef struct OCIDirPathDesc OCIDirPathDesc;
typedef struct OCIDirPathFuncCtx OCIDirPathFuncCtx;
typedef struct OCIDirPathStream OCIDirPathStream;
# 375 "/home/db2inst1/sqllib/include/db2ci.h"
typedef void (*pLgenfp_t)(void);
typedef struct cdaDef Cda_Def;
typedef struct cdaDef Lda_Def;







   typedef struct nzctx nzctx;
   typedef struct nzstrc nzstrc;
   typedef struct nzosContext nzosContext;
# 1988 "/home/db2inst1/sqllib/include/db2ci.h"
struct cda_head {
    sb2 v2_rc;
    ub2 ft;
    ub4 rpc;
    ub2 peo;
    ub1 fc;
    ub1 rcs1;
    ub2 rc;
    ub1 wrn;
    ub1 rcs2;
    sword rcs3;
    struct {
        struct {
           ub4 rcs4;
           ub2 rcs5;
           ub1 rcs6;
        } rd;
        ub4 rcs7;
        ub2 rcs8;
    } rid;
    sword ose;
    ub1 chk;
    void *pRcsp;
};


struct cdaDef {
    sb2 v2_rc;
    ub2 ft;
    ub4 rpc;
    ub2 peo;
    ub1 fc;
    ub1 rcs1;
    ub2 rc;
    ub1 wrn;
    ub1 rcs2;
    sword rcs3;
    struct {
        struct {
           ub4 rcs4;
           ub2 rcs5;
           ub1 rcs6;
        } rd;
        ub4 rcs7;
        ub2 rcs8;
    } rid;
    sword ose;
    ub1 chk;
    void *pRcsp;
    ub1 rcs9[64 - sizeof (struct cda_head)];
};

enum OCITypeGetOpt
{
  OCI_TYPEGET_HEADER,
  OCI_TYPEGET_ALL
};
typedef enum OCITypeGetOpt OCITypeGetOpt;


enum OCITypeEncap
{
  OCI_TYPEENCAP_PRIVATE,
  OCI_TYPEENCAP_PUBLIC
};
typedef enum OCITypeEncap OCITypeEncap;

struct OCIIOV
{
 void *bfp;
 ub4 bfl;
};

enum OCIPinOpt
{
  OCI_PIN_DEFAULT = 1,
  OCI_PIN_ANY = 3,
  OCI_PIN_RECENT = 4,
  OCI_PIN_LATEST = 5
};
typedef enum OCIPinOpt OCIPinOpt;


enum OCILockOpt
{
  OCI_LOCK_NONE = 1,
  OCI_LOCK_X = 2,
  OCI_LOCK_X_NOWAIT = 3
};
typedef enum OCILockOpt OCILockOpt;

enum OCIMarkOpt
{
  OCI_MARK_DEFAULT = 1,
  OCI_MARK_NONE = OCI_MARK_DEFAULT,
  OCI_MARK_UPDATE
};
typedef enum OCIMarkOpt OCIMarkOpt;

enum OCILobMode
{
  OCI_LOBMODE_READONLY = 1,
  OCI_LOBMODE_READWRITE = 2
};
typedef enum OCILobMode OCILobMode;






enum OCITypeMethodFlag
{
  OCI_TYPEMETHOD_ABSTRACT = 0x2000,
  OCI_TYPEMETHOD_CONSTANT = 0x0002,
  OCI_TYPEMETHOD_CONSTRUCTOR = 0x0008,
  OCI_TYPEMETHOD_DESTRUCTOR = 0x0010,
  OCI_TYPEMETHOD_INLINE = 0x0001,
  OCI_TYPEMETHOD_MAP = 0x0080,
  OCI_TYPEMETHOD_OPERATOR = 0x0020,
  OCI_TYPEMETHOD_ORDER = 0x0100,
  OCI_TYPEMETHOD_OVERRIDING = 0x4000,
  OCI_TYPEMETHOD_PIPELINED = 0x8000,
  OCI_TYPEMETHOD_RNDS= 0x0200,
  OCI_TYPEMETHOD_RNPS= 0x0800,
  OCI_TYPEMETHOD_SELFISH = 0x0040,
  OCI_TYPEMETHOD_VIRTUAL = 0x0004,
  OCI_TYPEMETHOD_WNDS= 0x0400,
  OCI_TYPEMETHOD_WNPS= 0x1000
};
typedef enum OCITypeMethodFlag OCITypeMethodFlag;

enum OCITypeParamMode
{
  OCI_TYPEPARAM_IN = 0,
  OCI_TYPEPARAM_OUT,
  OCI_TYPEPARAM_INOUT,
  OCI_TYPEPARAM_BYREF,
  OCI_TYPEPARAM_OUTNCPY,
  OCI_TYPEPARAM_INOUTNCPY
};
typedef enum OCITypeParamMode OCITypeParamMode;

struct OCINumber
{
  ub1 OCINumberPart[22];
};
typedef struct OCINumber OCINumber;







struct OCITime
{
  ub1 OCITimeHH;
  ub1 OCITimeMI;
  ub1 OCITimeSS;
};
typedef struct OCITime OCITime;

struct OCIDate
{
  sb2 OCIDateYYYY;
  ub1 OCIDateMM;
  ub1 OCIDateDD;
  OCITime OCIDateTime;
};
typedef struct OCIDate OCIDate;

typedef enum nzerror
{
  NZEROR_BIND_SUBKEY_COUNT = 28798,
  NZERROR_3DES_SELF_TEST_FAILED = 29151,
  NZERROR_AES_SELF_TEST_FAILED = 29165,
  NZERROR_ALREADY_INITIALIZED = 28770,
  NZERROR_ASNBadEncodingErr = 28866,
  NZERROR_ASNIntegerTooBigErr = 28867,
  NZERROR_ATTRIBUTE_FINISH_FAILED = 28795,
  NZERROR_ATTRIBUTE_INIT = 28794,
  NZERROR_AUTH_SHARED_MEMORY = 28799,
  NZERROR_BAD_FILE_ID = 28772,
  NZERROR_BAD_MAGIC_NUMBER = 28768,
  NZERROR_BAD_PARAMETER_METHOD = 28780,
  NZERROR_BAD_PASSWORD = 28852,
  NZERROR_BAD_VERSION_NUMBER = 28767,
  NZERROR_BadCipherSuite = 28884,
  NZERROR_BIND_SERVICE_ERROR = 43050,
  NZERROR_BINDING_CREATION_FAILED = 28777,
  NZERROR_BS_CERTOBJ_CREAT_FAILED = 29140,
  NZERROR_BS_DER_IMP_FAILED = 29141,
  NZERROR_CANNOT_MODIFY_AL = 43072,
  NZERROR_CERT_ALREADY_INSTALLED = 29225,
  NZERROR_CERT_CHAIN_CREATION = 29223,
  NZERROR_CERT_IN_CRL = 29178,
  NZERROR_CERT_IN_CRL_CHECK_FAILED = 29179,
  NZERROR_CERT_NOT_IN_CRL = 29177,
  NZERROR_CERTIFICATE_VERIFY = 28791,
  NZERROR_CertNotInstalled = 29000,
  NZERROR_CIC_ERR_BAD_CTX = 29094,
  NZERROR_CIC_ERR_BAD_INDEX = 29095,
  NZERROR_CIC_ERR_BAD_LENGTH = 29096,
  NZERROR_CIC_ERR_CODING_BAD_ENCODING = 29097,
  NZERROR_CIC_ERR_CODING_BAD_PEM = 29070,
  NZERROR_CIC_ERR_CODING_BASE = 29071,
  NZERROR_CIC_ERR_DER_BAD_ENCODING = 29072,
  NZERROR_CIC_ERR_DER_BAD_ENCODING_LENGTH = 29073,
  NZERROR_CIC_ERR_DER_BASE = 29074,
  NZERROR_CIC_ERR_DER_ELEMENT_TOO_LONG = 29075,
  NZERROR_CIC_ERR_DER_INDEFINITE_LENGTH = 29076,
  NZERROR_CIC_ERR_DER_NO_MORE_ELEMENTS = 29077,
  NZERROR_CIC_ERR_DER_OBJECT_TOO_LONG = 29078,
  NZERROR_CIC_ERR_DER_TAG_SIZE = 29079,
  NZERROR_CIC_ERR_DER_TIME_OUT_OF_RANGE = 29080,
  NZERROR_CIC_ERR_DER_UNUSED_BITS_IN_BIT_STR = 29081,
  NZERROR_CIC_ERR_GENERAL_BASE = 29082,
  NZERROR_CIC_ERR_HASH_BASE = 29083,
  NZERROR_CIC_ERR_ILLEGAL_PARAM = 29084,
  NZERROR_CIC_ERR_MEM_NOT_OURS = 29085,
  NZERROR_CIC_ERR_MEM_OVERRUN = 29086,
  NZERROR_CIC_ERR_MEM_UNDERRUN = 29087,
  NZERROR_CIC_ERR_MEM_WAS_FREED = 29088,
  NZERROR_CIC_ERR_NO_PTR = 29091,
  NZERROR_CIC_ERR_NOT_FOUND = 29090,
  NZERROR_CIC_ERR_PKCS_AUTH_FAILED = 29054,
  NZERROR_CIC_ERR_PKCS_BAD_CONTENT_TYPE = 29055,
  NZERROR_CIC_ERR_PKCS_BAD_INPUT = 29056,
  NZERROR_CIC_ERR_PKCS_BAD_PADDING = 29057,
  NZERROR_CIC_ERR_PKCS_BAD_SN = 29058,
  NZERROR_CIC_ERR_PKCS_BAD_SnLen = 29059,
  NZERROR_CIC_ERR_PKCS_BAD_VERSION = 29060,
  NZERROR_CIC_ERR_PKCS_BASE = 29061,
  NZERROR_CIC_ERR_PKCS_FIELD_NOT_PRESENT = 29062,
  NZERROR_CIC_ERR_PKCS_NEED_CERTVAL = 29063,
  NZERROR_CIC_ERR_PKCS_NEED_PASSWORD = 29064,
  NZERROR_CIC_ERR_PKCS_NEED_PKC = 29065,
  NZERROR_CIC_ERR_PKCS_NEED_PRV_KEY = 29066,
  NZERROR_CIC_ERR_PKCS_NEED_TRUSTED = 29067,
  NZERROR_CIC_ERR_PKCS_UNSUP_PRVKEY_TYPE = 29069,
  NZERROR_CIC_ERR_PKCS_UNSUPPORTED_CERT_FORMAT = 29068,
  NZERROR_CIC_ERR_PKCS12_MISSING_ALG = 29053,
  NZERROR_CIC_ERR_RANDOM = 29135,
  NZERROR_CIC_ERR_SMALL_BUFFER = 29136,
  NZERROR_CIC_ERR_SSL_ALERT_CB_FAILURE = 29004,
  NZERROR_CIC_ERR_SSL_BAD_CERTIFICATE = 29005,
  NZERROR_CIC_ERR_SSL_BAD_CERTIFICATE_REQUEST = 29006,
  NZERROR_CIC_ERR_SSL_BAD_CLEAR_KEY_LEN = 29007,
  NZERROR_CIC_ERR_SSL_BAD_CONTEXT = 29137,
  NZERROR_CIC_ERR_SSL_BAD_DHPARAM_KEY_LENGTH = 29008,
  NZERROR_CIC_ERR_SSL_BAD_ENCRYPTED_KEY_LEN = 29009,
  NZERROR_CIC_ERR_SSL_BAD_EXPORT_KEY_LENGTH = 29010,
  NZERROR_CIC_ERR_SSL_BAD_FINISHED_MESSAGE = 29011,
  NZERROR_CIC_ERR_SSL_BAD_KEY_ARG_LEN = 29012,
  NZERROR_CIC_ERR_SSL_BAD_MAC = 29013,
  NZERROR_CIC_ERR_SSL_BAD_MAX_FRAGMENT_LENGTH_EXTENSION = 29014,
  NZERROR_CIC_ERR_SSL_BAD_MESSAGE_LENGTH = 29015,
  NZERROR_CIC_ERR_SSL_BAD_PKCS1_PADDING = 29016,
  NZERROR_CIC_ERR_SSL_BAD_PREMASTER_SECRET_LENGTH = 29017,
  NZERROR_CIC_ERR_SSL_BAD_PREMASTER_SECRET_VERSION = 29018,
  NZERROR_CIC_ERR_SSL_BAD_PROTOCOL_VERSION = 29019,
  NZERROR_CIC_ERR_SSL_BAD_RECORD_LENGTH = 29020,
  NZERROR_CIC_ERR_SSL_BAD_SECRET_KEY_LEN = 29021,
  NZERROR_CIC_ERR_SSL_BAD_SIDE = 29022,
  NZERROR_CIC_ERR_SSL_BUFFERS_NOT_EMPTY = 29023,
  NZERROR_CIC_ERR_SSL_CERT_CHECK_CALLBACK = 29025,
  NZERROR_CIC_ERR_SSL_CERTIFICATE_VALIDATE_FAILED = 29024,
  NZERROR_CIC_ERR_SSL_DECRYPT_FAILED = 29026,
  NZERROR_CIC_ERR_SSL_ENTROPY_COLLECTION = 29027,
  NZERROR_CIC_ERR_SSL_FAIL_SERVER_VERIFY = 29028,
  NZERROR_CIC_ERR_SSL_HANDSHAKE_ALREADY_COMPLETED = 29029,
  NZERROR_CIC_ERR_SSL_HANDSHAKE_REQUESTED = 29030,
  NZERROR_CIC_ERR_SSL_HANDSHAKE_REQUIRED = 29031,
  NZERROR_CIC_ERR_SSL_INCOMPLETE_IDENTITY = 29032,
  NZERROR_CIC_ERR_SSL_INVALID_PFX = 29033,
  NZERROR_CIC_ERR_SSL_NEEDS_CIPHER_OR_CLIENTAUTH = 29034,
  NZERROR_CIC_ERR_SSL_NEEDS_PRNG = 29035,
  NZERROR_CIC_ERR_SSL_NO_CERTIFICATE = 29037,
  NZERROR_CIC_ERR_SSL_NO_CLIENT_AUTH_MODES = 29098,
  NZERROR_CIC_ERR_SSL_NO_MATCHING_CERTIFICATES = 29038,
  NZERROR_CIC_ERR_SSL_NO_MATCHING_CIPHER_SUITES = 29039,
  NZERROR_CIC_ERR_SSL_NO_SUPPORTED_CIPHER_SUITES = 29040,
  NZERROR_CIC_ERR_SSL_NOT_SUPPORTED = 29036,
  NZERROR_CIC_ERR_SSL_NULL_CB = 29041,
  NZERROR_CIC_ERR_SSL_READ_BUFFER_NOT_EMPTY = 29042,
  NZERROR_CIC_ERR_SSL_READ_REQUIRED = 29043,
  NZERROR_CIC_ERR_SSL_RENEGOTIATION_ALREADY_REQUESTED = 29044,
  NZERROR_CIC_ERR_SSL_RENEGOTIATION_REFUSED = 29045,
  NZERROR_CIC_ERR_SSL_RESUMABLE_SESSION = 29046,
  NZERROR_CIC_ERR_SSL_TLS_EXTENSION_MISMATCH = 29047,
  NZERROR_CIC_ERR_SSL_UNEXPECTED_MSG = 29048,
  NZERROR_CIC_ERR_SSL_UNKNOWN_RECORD = 29049,
  NZERROR_CIC_ERR_SSL_UNSUPPORTED_CLIENT_AUTH_MODE = 29050,
  NZERROR_CIC_ERR_SSL_UNSUPPORTED_PUBKEY_TYPE = 29051,
  NZERROR_CIC_ERR_SSL_WRITE_BUFFER_NOT_EMPTY = 29052,
  NZERROR_CIC_ERR_TIMEOUT = 29092,
  NZERROR_CIC_ERR_UNIT_MASK = 29093,
  NZERROR_CKEYPAIR_SELF_TEST_FAILED = 29155,
  NZERROR_CREATE_KEY_OBJ_ERROR = 43051,
  NZERROR_CREATE_PKCS10_OBJECT = 43053,
  NZERROR_CRL_EXPIRED = 29195,
  NZERROR_CRL_NOT_IN_CACHE = 29194,
  NZERROR_CRL_SIG_VERIFY_FAILED = 29176,
  NZERROR_CRLDP_NO_CRL_FOUND = 29193,
  NZERROR_CRNG_SELF_TEST_FAILED = 29156,
  NZERROR_DAC_SELF_TEST_FAILED = 29161,
  NZERROR_DATA_SOURCE_INIT_FAILED = 28752,
  NZERROR_DATA_SOURCE_TERM_FAILED = 28753,
  NZERROR_DECRYPT_FAILED = 28786,
  NZERROR_DES_SELF_TEST_FAILED = 29150,
  NZERROR_DN_MATCH = 29222,
  NZERROR_DRNG_SELF_TEST_FAILED = 29154,
  NZERROR_ELEMENT_ADD_FAILED = 28762,
  NZERROR_ENCRYPT_FAILED = 28787,
  NZERROR_EntrustGetInfoFailed = 28891,
  NZERROR_EntrustGetNameFailed = 28893,
  NZERROR_EntrustLoadCertificateFailed = 28892,
  NZERROR_EntrustLoginFailed = 28890,
  NZERROR_EXTERNAL_PKCS12_NOT_SUPPORTED_FIPS_MODE = 29164,
  NZERROR_FILE_BAD_PERMISSION = 28842,
  NZERROR_FILE_CLOSE_FAILED = 28775,
  NZERROR_FILE_NAME_TRANSLATION = 28784,
  NZERROR_FILE_OPEN_FAILED = 28759,
  NZERROR_FILE_OSD_ERROR = 28843,
  NZERROR_FILE_WRITE_FAILED = 28774,
  NZERROR_FIPS_LIB_DIFFERS = 29160,
  NZERROR_FIPS_LIB_OPEN_FAILED = 29158,
  NZERROR_FIPS_LIB_READ_ERROR = 29159,
  NZERROR_FIPS_PATHNAME_ERROR = 29157,
  NZERROR_GENERIC = 28750,
  NZERROR_GET_CERT_FIELDS = 43052,
  NZERROR_GET_PKCS10_DER = 43056,
  NZERROR_ImproperClientCredentials = 28878,
  NZERROR_ImproperServerCredentials = 28877,
  NZERROR_INITIALIZATION_FAILED = 28853,
  NZERROR_INITIALIZE_CERTC = 43057,
  NZERROR_INSERT_PRIVATE_KEY = 43058,
  NZERROR_INVALID_CERT_STATUS_PROTOCOL = 29180,
  NZERROR_INVALID_HEADER_LENGTH = 43070,
  NZERROR_INVALID_INPUT = 28788,
  NZERROR_INVALID_KEY_DATA_TYPE = 28797,
  NZERROR_INVALID_PARAMETER = 28783,
  NZERROR_KD_CREATE = 28810,
  NZERROR_LAST_ERROR = 43499,
  NZERROR_LDAP_BIND_FAILED = 29182,
  NZERROR_LDAP_COUNT_ENTRIES_FAILED = 29188,
  NZERROR_LDAP_FIRSTATTR_FAILED = 29185,
  NZERROR_LDAP_GETVALUESLEN_FAILED = 29186,
  NZERROR_LDAP_MULTIPLE_ENTRIES_FOUND = 29190,
  NZERROR_LDAP_NO_ENTRY_FOUND = 29189,
  NZERROR_LDAP_OPEN_FAILED = 29181,
  NZERROR_LDAP_RESULT_FAILED = 29184,
  NZERROR_LDAP_SEARCH_FAILED = 29183,
  NZERROR_LDAP_UNSUPPORTED_VALMEC = 29187,
  NZERROR_LDAP_VALMEC_NOT_SET = 29192,
  NZERROR_LIST_CREATION_FAILED = 28760,
  NZERROR_LOCKEYID_CREATE_FAILED = 29100,
  NZERROR_MEMORY_ALLOC_0_BYTES = 28757,
  NZERROR_MEMORY_ALLOC_FAILED = 28756,
  NZERROR_MEMORY_FREE_FAILED = 28758,
  NZERROR_METHOD_NOT_FOUND = 28769,
  NZERROR_MUTEX_DESTROY_FAILED = 29139,
  NZERROR_MUTEX_INITIALIZE_FAILED = 29138,
  NZERROR_NAME_TYPE_NOT_FOUND = 28789,
  NZERROR_NLS_STRING_OPEN_FAILED = 28790,
  NZERROR_NO_CERTIFICATE_ALERT = 28845,
  NZERROR_NO_CLEAR_PRIVATE_KEY_FILE = 28847,
  NZERROR_NO_CLEAR_TRUSTPOINT_FILE = 28850,
  NZERROR_NO_ELEMENT = 28761,
  NZERROR_NO_ENCRYPTED_PRIVATE_KEY_FILE = 28848,
  NZERROR_NO_ENCRYPTED_TRUSTPOINT_FILE = 28851,
  NZERROR_NO_LIST = 28765,
  NZERROR_NO_MATCHING_CERT_REQ = 29224,
  NZERROR_NO_MATCHING_PRIVATE_KEY = 29226,
  NZERROR_NO_MEMORY = 28751,
  NZERROR_NO_PRIVATE_KEY = 28846,
  NZERROR_NO_SUCH_PARAMETER = 28785,
  NZERROR_NO_TRUSTPOINTS = 28849,
  NZERROR_NO_WALLET = 28844,
  NZERROR_NoKeyPairForKeyUsage = 28885,
  NZERROR_NONFIPS_CIPHERSUITE = 29162,
  NZERROR_NoProtocolSideSet = 28879,
  NZERROR_NOT_ALLOCATED = 28782,
  NZERROR_NOT_INITIALIZED = 28771,
  NZERROR_OBJECT_GET_FAILED = 28755,
  NZERROR_OBJECT_STORE_FAILED = 28754,
  NZERROR_OCI_BIND_FAILED = 28793,
  NZERROR_OCI_PLSQL_FAILED = 28792,
  NZERROR_OID_INFO_NOT_SET = 29191,
  NZERROR_OK = 0,
  NZERROR_OUTPUT_BUFFER_TOO_SMALL = 28776,
  NZERROR_P12_ADD_ATTRIB_FAILED = 29117,
  NZERROR_P12_ADD_CERT_FAILED = 29102,
  NZERROR_P12_ADD_CERTREQ_FAILED = 29104,
  NZERROR_P12_ADD_PKCS11INFO_FAILED = 29122,
  NZERROR_P12_ADD_PVTKEY_FAILED = 29101,
  NZERROR_P12_ADD_SECRET_FAILED = 29121,
  NZERROR_P12_CREATE_FAILED = 29107,
  NZERROR_P12_CRT_ATTRIB_FAILED = 29118,
  NZERROR_P12_DEST_FAILED = 29107,
  NZERROR_P12_EXP_CERT_FAILED = 29120,
  NZERROR_P12_EXP_PVTKEY_FAILED = 29115,
  NZERROR_P12_GET_ATTRIB_FAILED = 29116,
  NZERROR_P12_GET_CERT_FAILED = 29112,
  NZERROR_P12_GET_PKCS11INFO_FAILED = 29123,
  NZERROR_P12_GET_PVTKEY_FAILED = 29113,
  NZERROR_P12_IMP_CERT_FAILED = 29119,
  NZERROR_P12_IMP_PVTKEY_FAILED = 29114,
  NZERROR_P12_INVALID_BAG = 29110,
  NZERROR_P12_INVALID_INDEX = 29111,
  NZERROR_P12_MULTIPLE_PKCS11_CERTLABEL = 29131,
  NZERROR_P12_MULTIPLE_PKCS11_LIBNAME = 29124,
  NZERROR_P12_MULTIPLE_PKCS11_TOKENLABEL = 29125,
  NZERROR_P12_MULTIPLE_PKCS11_TOKENPASSPHRASE = 29126,
  NZERROR_P12_PKCS11_LIBNAME_NOT_SET = 29128,
  NZERROR_P12_PKCS11_TOKENLABEL_NOT_SET = 29129,
  NZERROR_P12_PKCS11_TOKENPASSPHRASE_NOT_SET = 29130,
  NZERROR_P12_PVTKEY_CRT_FAILED = 29109,
  NZERROR_P12_RAND_ERROR = 29108,
  NZERROR_P12_UNKNOWN_PKCS11INFO = 29127,
  NZERROR_P12_WLT_CREATE_FAILED = 29103,
  NZERROR_P12_WLT_EXP_FAILED = 29105,
  NZERROR_P12_WLT_IMP_FAILED = 29106,
  NZERROR_PARAMETER_BAD_TYPE = 28763,
  NZERROR_PARAMETER_MALFORMED = 28778,
  NZERROR_PARAMETER_NO_DATA = 28781,
  NZERROR_PARAMETER_NO_METHOD = 28779,
  NZERROR_PARAMETER_RETRIEVAL = 28764,
  NZERROR_PKCS11_BAD_PASSPHRASE = 43002,
  NZERROR_PKCS11_CERT_WITH_LABEL_NOT_USER_CERT = 43021,
  NZERROR_PKCS11_CHANGE_PROVIDERS_ERROR = 43013,
  NZERROR_PKCS11_CREATE_KEYPAIR_ERROR = 43015,
  NZERROR_PKCS11_GET_FUNC_LIST = 43003,
  NZERROR_PKCS11_GET_PRIVATE_KEY_ERROR = 43014,
  NZERROR_PKCS11_GET_SLOT_LIST = 43006,
  NZERROR_PKCS11_GET_TOKEN_INFO = 43008,
  NZERROR_PKCS11_INITIALIZE = 43004,
  NZERROR_PKCS11_LIBRARY_NOT_FOUND = 43000,
  NZERROR_PKCS11_MULTIPLE_CERTS_ON_TOKEN_WITH_GIVEN_LABEL = 43020,
  NZERROR_PKCS11_NO_CERT_ON_TOKEN = 43017,
  NZERROR_PKCS11_NO_CERT_ON_TOKEN_WITH_GIVEN_LABEL = 43019,
  NZERROR_PKCS11_NO_TOKENS_PRESENT = 43005,
  NZERROR_PKCS11_NO_USER_CERT_ON_TOKEN = 43018,
  NZERROR_PKCS11_SYMBOL_NOT_FOUND = 43009,
  NZERROR_PKCS11_TOKEN_LOGIN_FAILED = 43011,
  NZERROR_PKCS11_TOKEN_NOT_FOUND = 43001,
  NZERROR_PKCS11_WALLET_CONTAINS_P11_INFO = 43016,
  NZERROR_PLSQL_ORACLE_TO_REAL = 28814,
  NZERROR_PLSQL_REAL_TO_ORACLE = 28815,
  NZERROR_RIO_ACCESS_DESCRIPTOR = 28811,
  NZERROR_RIO_CLOSE = 28804,
  NZERROR_RIO_DELETE = 28809,
  NZERROR_RIO_INFO = 28808,
  NZERROR_RIO_IO = 28803,
  NZERROR_RIO_MODE = 28802,
  NZERROR_RIO_OBJECT_TYPE = 28801,
  NZERROR_RIO_OPEN = 28800,
  NZERROR_RIO_RECORD = 28812,
  NZERROR_RIO_RECORD_TYPE = 28813,
  NZERROR_RIO_RETRIEVE = 28805,
  NZERROR_RIO_STORE = 28806,
  NZERROR_RIO_UPDATE = 28807,
  NZERROR_RSA_ERROR = 43059,
  NZERROR_RSA_SELF_TEST_FAILED = 29153,
  NZERROR_ServerDNMisConfigured = 29003,
  NZERROR_ServerDNMisMatched = 29002,
  NZERROR_SET_PKCS10_FIELDS = 43054,
  NZERROR_setCertFailed = 28881,
  NZERROR_setPersonaFailed = 28880,
  NZERROR_setTPFailed = 28883,
  NZERROR_setVKeyFailed = 28882,
  NZERROR_SHA_SELF_TEST_FAILED = 29152,
  NZERROR_SIGN_CERT_REQUEST = 43055,
  NZERROR_SLTSCTX_INIT_FAILED = 43060,
  NZERROR_SLTSCTX_TERM_FAILED = 43062,
  NZERROR_SLTSKYC_FAILED = 43061,
  NZERROR_SLTSKYS_FAILED = 43063,
  NZERROR_SSLBadParameterErr = 28873,
  NZERROR_SSLConnectionClosedError = 28865,
  NZERROR_SSLConnectionClosedGraceful = 28864,
  NZERROR_SSLFatalAlert = 28860,
  NZERROR_SSLIOClosedOverrideGoodbyeKiss = 28874,
  NZERROR_SSLIOErr = 28862,
  NZERROR_SSLMemoryErr = 28854,
  NZERROR_SSLNegotiationErr = 28859,
  NZERROR_SSLOverflowErr = 28856,
  NZERROR_SSLProtocolErr = 28858,
  NZERROR_SSLSessionNotFoundErr = 28863,
  NZERROR_SSLUnknownErr = 28857,
  NZERROR_SSLUnsupportedErr = 28855,
  NZERROR_SSLWouldBlockErr = 28861,
  NZERROR_TERMINATE_FAIL = 28766,
  NZERROR_THIS_MUST_BE_LAST,
  NZERROR_TK_ALGORITHM = 29240,
  NZERROR_TK_BAD_HEADER_LENGTH = 29230,
  NZERROR_TK_BAD_MAGIC_NUMBER = 29229,
  NZERROR_TK_BYTES_NEEDED = 29228,
  NZERROR_TK_CANNOT_GROW = 29241,
  NZERROR_TK_CE_ALGAPPLY = 29238,
  NZERROR_TK_CE_ALGFINISH = 29237,
  NZERROR_TK_CE_ALGINIT = 29239,
  NZERROR_TK_CE_DECODE_KEY = 29234,
  NZERROR_TK_CE_ENCODE_KEY = 29233,
  NZERROR_TK_CE_FUNC = 29245,
  NZERROR_TK_CE_GEYKEYINFO = 29235,
  NZERROR_TK_CE_INIT = 29231,
  NZERROR_TK_CE_KEYINIT = 29232,
  NZERROR_TK_CE_STATE = 29249,
  NZERROR_TK_INV_CIPHR_TYPE = 28827,
  NZERROR_TK_INV_DATA_FMT = 28832,
  NZERROR_TK_INV_ENG_FUNC = 28826,
  NZERROR_TK_INV_IDENT_TYPE = 28828,
  NZERROR_TK_INVALID_STATE = 28824,
  NZERROR_TK_KEYSIZE = 29242,
  NZERROR_TK_KEYTYPE = 29243,
  NZERROR_TK_NOTOPEN = 29247,
  NZERROR_TK_PLSQL_BUF_TOO_SMALL = 28834,
  NZERROR_TK_PLSQL_GENERIC = 28817,
  NZERROR_TK_PLSQL_INSUFF_INFO = 28833,
  NZERROR_TK_PLSQL_INV_IDENT_DESC = 28835,
  NZERROR_TK_PLSQL_NO_BYTES = 28823,
  NZERROR_TK_PLSQL_NO_CIPHER_TYPE = 28829,
  NZERROR_TK_PLSQL_NO_CONTEXT = 28818,
  NZERROR_TK_PLSQL_NO_DATA_FMT = 28831,
  NZERROR_TK_PLSQL_NO_DIST_NAME = 28819,
  NZERROR_TK_PLSQL_NO_ENG_FUNC = 28825,
  NZERROR_TK_PLSQL_NO_IDENT_TYPE = 28830,
  NZERROR_TK_PLSQL_NO_IDENTITY = 28838,
  NZERROR_TK_PLSQL_NO_INPUT = 28821,
  NZERROR_TK_PLSQL_NO_PASSWORD = 28816,
  NZERROR_TK_PLSQL_NO_PERSONA = 28839,
  NZERROR_TK_PLSQL_NO_SEED = 28822,
  NZERROR_TK_PLSQL_NO_STATE = 28820,
  NZERROR_TK_PLSQL_NO_WALLET = 28837,
  NZERROR_TK_PLSQL_NO_WRL = 29244,
  NZERROR_TK_PLSQL_WALLET_NOTOPEN = 28836,
  NZERROR_TK_PLSQL_WALLET_OPEN = 28840,
  NZERROR_TK_SEED_RANDOM = 29236,
  NZERROR_TK_TDU_FORMAT = 29246,
  NZERROR_TK_WRLTYPE = 29248,
  NZERROR_UNSUPPORTED = 28841,
  NZERROR_UNSUPPORTED_METHOD = 28796,
  NZERROR_VALIDITY_EXPIRED = 29227,
  NZERROR_VENDOR_NOT_SUPPORTED_FIPS_MODE = 29163,
  NZERROR_WALLET_CONTAINS_USER_CREDENTIALS = 43071,
  NZERROR_WRITE_MAGIC_VERSION = 28773,
  NZERROR_X509CertChainIncompleteErr = 28871,
  NZERROR_X509CertChainInvalidErr = 28868,
  NZERROR_X509CertExpiredErr = 28869,
  NZERROR_X509DataNotFoundErr = 28872,
  NZERROR_X509IESGCErr = 28876,
  NZERROR_X509MozillaSGCErr = 28875,
  NZERROR_X509NamesNotEqualErr = 28870
} nzerror;

enum nzttwrl
{
   NZTTWRL_DEFAULT = 1,
   NZTTWRL_SQLNET,
   NZTTWRL_FILE,
   NZTTWRL_ENTR,
   NZTTWRL_MCS,
   NZTTWRL_ORACLE,
   NZTTWRL_NULL,
   NZTTWRL_REGISTRY
};
typedef enum nzttwrl nzttwrl;


enum nzttces
{
   NZTTCES_CONTINUE = 1,
   NZTTCES_END,
   NZTTCES_RESET
};
typedef enum nzttces nzttces;

enum nzttcef
{
   NZTTCEF_DETACHEDSIGNATURE = 1,
   NZTTCEF_SIGNATURE,
   NZTTCEF_ENVELOPING,
   NZTTCEF_PKENCRYPTION,
   NZTTCEF_ENCRYPTION,
   NZTTCEF_KEYEDHASH,
   NZTTCEF_HASH,
   NZTTCEF_RANDOM,

   NZTTCEF_LAST
};
typedef enum nzttcef nzttcef;

enum nzttState
{
   NZTTSTATE_EMPTY = 0,
   NZTTSTATE_REQUESTED,
   NZTTSTATE_REAday,
   NZTTSTATE_INVALID,
   NZTTSTATE_RENEWAL
};
typedef enum nzttState nzttState;

enum nzttVersion
{
   NZTTVERSION_X509v1 = 1,
   NZTTVERSION_X509v3,



   NZTTVERSION_INVALID_TYPE
};
typedef enum nzttVersion nzttVersion;

enum nzttCipherType
{
   NZTTCIPHERTYPE_RSA = 1,
   NZTTCIPHERTYPE_DES,
   NZTTCIPHERTYPE_RC4,
   NZTTCIPHERTYPE_MD5DES,
   NZTTCIPHERTYPE_MD5RC2,
   NZTTCIPHERTYPE_MD5,
   NZTTCIPHERTYPE_SHA
};
typedef enum nzttCipherType nzttCipherType;

enum nztttdufmt
{
   NZTTTDUFMT_PKCS7 = 1,
   NZTTTDUFMT_RSAPAD,
   NZTTTDUFMT_ORACLEv1,
   NZTTTDUFMT_LAST
};
typedef enum nztttdufmt nztttdufmt;

enum nzttValState
{
   NZTTVALSTATE_NONE = 1,
   NZTTVALSTATE_GOOD,
   NZTTVALSTATE_REVOKED
};
typedef enum nzttValState nzttValState;

enum nzttPolicy
{
   NZTTPOLICY_NONE = 0,
   NZTTPOLICY_RETRY_1,
   NZTTPOLICY_RETRY_2,
   NZTTPOLICY_RETRY_3
};
typedef enum nzttPolicy nzttPolicy;
# 2650 "/home/db2inst1/sqllib/include/db2ci.h"
typedef ub1 nzttID[16];

enum nzttIdentType
{
   NZTTIDENTITYTYPE_INVALID_TYPE = 0,
   NZTTIDENTITYTYPE_CERTIFICTAE,
   NZTTIDENTITYTYPE_CERT_REQ,
   NZTTIDENTITYTYPE_RENEW_CERT_REQ,
   NZTTIDENTITYTYPE_CLEAR_ETP,
   NZTTIDENTITYTYPE_CLEAR_UTP,
   NZTTIDENTITYTYPE_CLEAR_PTP
};
typedef enum nzttIdentType nzttIdentType;

struct nzttBufferBlock
{


   uword flagsNzttBufferBlock;
   ub4 bufLenNzttBufferBlock;
   ub4 usedlenNzttBufferBlock;
   ub1 *bufferNzttBufferBlock;
};
typedef struct nzttBufferBlock nzttBufferBlock;

struct nzttWallet
{
   ub1 *pLdapNameNzttWallet;
   ub4 ldapNamelenNzttWallet;
   nzttPolicy securePolicyNzttWallet;
   nzttPolicy openPolicyNzttWallet;
   nzttPersona *pPersonaNzttWallet;
   nzttWalletPrivate *pRivateNzttWallet;



};

struct nzttPersona
{
   ub1 *genericNameNzttPersona;
   ub4 genericNamelenNzttPersona;
   nzttPersonaPrivate *pRivateNzttPersona;
   nzttIdentity *pMycertreqsNzttPersona;
   nzttIdentity *pMycertsNzttPersona;
   nzttIdentity *pMytpsNzttPersona;
   nzssEntry *pMystoreNzttPersona;
   nzpkcs11_Info *pMypkcs11InfoNzttPersona;
   struct nzttPersona *pNextNzttPersona;





};

struct nzttIdentity
{
   text *dnNzttIdentity;
   ub4 dnlenNzttIdentity;
   text *pCommentNzttIdentity;
   ub4 commentlenNzttIdentity;
   nzttIdentityPrivate *pRivateNzttIdentity;
   nzttIdentity *pNextNzttIdentity;
};

struct nzttB64Cert
{
   ub1 *b64CertNzttB64Cert;
   ub4 b64CertlenNzttB64Cert;
   struct nzttB64Cert *pNextNzttB64Cert;
};
typedef struct nzttB64Cert nzttB64Cert;


struct nzttPKCS7ProtInfo
{
   nzttCipherType mictypeNzttPKCS7ProtInfo;
   nzttCipherType symmtypeNzttPKCS7ProtInfo;
   ub4 keylenNzttPKCS7ProtInfo;
};
typedef struct nzttPKCS7ProtInfo nzttPKCS7ProtInfo;

union nzttProtInfo
{
   nzttPKCS7ProtInfo pkcs7NzttProtInfo;
};
typedef union nzttProtInfo nzttProtInfo;

struct nzttPersonaDesc
{
   ub4 privlenNzttPersonaDesc;
   ub1 *pRivNzttPersonaDesc;
   ub4 prllenNzttPersonaDesc;
   text *pRlNzttPersonaDesc;
   ub4 aliaslenNzttPersonaDesc;
   text *aliasNzttPersonaDesc;
   ub4 longlenNzttPersonaDesc;
   text *pLongNzttPersonaDesc;
};
typedef struct nzttPersonaDesc nzttPersonaDesc;

struct nzttIdentityDesc
{
   ub4 publenNzttIdentityDesc;
   ub1 *pPubNzttIdentityDesc;
   ub4 dnlenNzttIdentityDesc;
   text *dnNzttIdentityDesc;
   ub4 longlenNzttIdentityDesc;
   text *pLongNzttIdentityDesc;
   ub4 quallenNzttIdentityDesc;
   text *pTrustqualNzttIdentityDesc;
};
typedef struct nzttIdentityDesc nzttIdentityDesc;
# 2779 "/home/db2inst1/sqllib/include/db2ci.h"
typedef void * (*OCICallbackMalloc)( void * ctxp, size_t size );
typedef void * (*OCICallbackRaloc)( void * ctxp, void * memp, size_t newsize );
typedef void (*OCICallbackFree)( void * ctxp, void * memptr );
typedef sb4 (*OCICallbackInBind)(void *pIctxp, OCIBind *pBindp, ub4 iter,
                                  ub4 index, void **pBufpp, ub4 *pAlenp,
                                  ub1 *pPiecep, void **pIndp);

typedef sb4 (*OCICallbackOutBind)(void *pOctxp, OCIBind *pBindp, ub4 iter,
                                 ub4 index, void **pBufpp, ub4 **pAlenp,
                                 ub1 *pPiecep, void **pIndp,
                                 ub2 **pRcodep);

typedef sb4 (*OCICallbackDefine)(void *pOctxp, OCIDefine *pDefnp, ub4 iter,
                                 void **pBufpp, ub4 **pAlenp, ub1 *pPiecep,
                                 void **pIndp, ub2 **pRcodep);

typedef sword (*OCIUserCallback)(void *pCtxp, void *pHndlp, ub4 type,
                                 ub4 fcode, ub4 when, sword returnCode,
                                 sb4 *pErrnop, va_list arglist);

typedef sword (*OCIEnvCallbackType)( OCIEnv *pEnvironment, ub4 mode,
                                   size_t xtramemSz, void *pUsrmemp,
                                   OCIUcb *pUcbDesc);

typedef sb4 (*OCICallbackLobRead)(void *pCtxp, const void *pBufp,
                                             ub4 Len, ub1 piece);

typedef sb4 (*OCICallbackLobWrite)(void *pCtxp, void *pBufp,
                                          ub4 *pLenp, ub1 *pPiece);


typedef sb4 (*OCICallbackLobRead2)(void *pCtxp, const void *pBufp, oraub8 Len,
                                   ub1 piece, void **pChangedbUfpp,
                                   oraub8 *pChangedLenp);

typedef sb4 (*OCICallbackLobWrite2)(void *pCtxp, void *pBufp, oraub8 *pLenp,
                                    ub1 *pPiece, void **pChangedbUfpp,
                                    oraub8 *pChangedLenp);

typedef sb4 (*OCICallbackLobArrayRead)(void *pCtxp, ub4 arrayIter,
                                       const void *pBufp, oraub8 Len,
                                       ub1 piece, void **pChangedbUfpp,
                                       oraub8 *pChangedLenp);

typedef sb4 (*OCICallbackLobArrayWrite)(void *pCtxp, ub4 arrayIter,
                                        void *pBufp, oraub8 *pLenp,
                                        ub1 *pPiece, void **pChangedbUfpp,
                                        oraub8 *pChangedLenp);

typedef sb4 (*OCICallbackLobGetDeduplicateRegions)(void *pCtxp,
                                              OCILobRegion *pRegions,
                                              ub4 count, ub1 piece,
                                              OCILobRegion **pChangedReg,
                                              ub4 *pChangedCount);

typedef sb4 (*OCICallbackAQEnq)(void *pCtxp, void **pPayload,
                                void **pPayloadInd);

typedef sb4 (*OCICallbackAQEnqStreaming)(void *pCtxp, void **pPayload,
                                         void **pPayloadInd,
                                         OCIAQMsgProperties **pMsgprop,
                                         OCIType **pPinnedType);

typedef sb4 (*OCICallbackAQDeq)(void *pCtxp,
                                void **pPayload,
                                void **pPayloadInd);




typedef sb4 (*OCICallbackFailover)(void *pSvcctx, void *pEnvctx,
                                   void *pFoCtx, ub4 foType,
                                   ub4 foEvent);

typedef struct
{
  OCICallbackFailover callback_function;
  void *fo_ctx;
}
OCIFocbkStruct;

typedef sword (*OCICallbackStmtCache)(void *pCtx, OCIStmt *pStmt, ub4 mode);

typedef void (*OCIEventCallback)(void *pEvtctx, OCIEvent *pEventhp);




typedef sword (*OCIBindRowCallback)(void *pCtx);
typedef sword (*OCIFetchRowCallback)(void *pCtx);



typedef ub4 (*OCISubscriptionNotify)(void *pCtx,
                                     OCISubscription *pSubscrhp,
                                     void *pPay,
                                     ub4 payl,
                                     void *pDesc,
                                     ub4 mode);

typedef enum
{
  XCTXINIT_OCIDUR = 1,
  XCTXINIT_ERRHDL = 2
} ocixmldbpname;

typedef struct ocixmldbparam
{
  ocixmldbpname name_ocixmldbparam;
  void *pValue_ocixmldbparam;
} ocixmldbparam;







sword OCITypeByName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
          const OCISvcCtx *pServiceContext,
          const oratext *pSchemaName,
          ub4 sLength,
          const oratext *pTypeName,
          ub4 tLength,
          const oratext *pVersionName,
          ub4 vLen,
          OCIDuration pinDur,
          OCITypeGetOpt getOpt,
           OCIType **pPinnedType);

sword OCITypeMethodByName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                const OCIType *pPinnedType,
                                const oratext *pMethodName,
                                ub4 mLen,
                                 OCITypeMethod **pMethodsType);

sword OCITypeMethodNext( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                  OCITypeIter *pIterNextMethod,
                               OCITypeMethod **pMethodType);

sword OCITypeArrayByName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
          const OCISvcCtx *pServiceContext,
          ub4 arrayLength,
          const oratext *pSchemaName[],
          ub4 sLength[],
          const oratext *pTypeName[],
          ub4 tLength[],
          const oratext *pVersionName[],
          ub4 vLen[],
          OCIDuration pinDur,
          OCITypeGetOpt getOpt,
           OCIType **pPinnedType);

sword OCITypeIterNew( OCIEnv *pEnvironment,
                               OCIError *pErrorHandle,
                            OCIType *pPinnedType,
                                OCITypeIter **pIterOrt);

sword OCITypeAttrNext( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                OCITypeIter *pIterAttr,
                             OCITypeElem **pTypeElement);


sword OCITypeCollElem( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCIType *pPinnedType,
                                OCITypeElem **pTypeElement);

sword OCITypeIterSet( OCIEnv *pEnvironment,
                               OCIError *pErrorHandle,
                           OCIType *pPinnedType,
                                OCITypeIter *pIterOrt);

sword OCITypeIterFree( OCIEnv *pEnvironment,
                                OCIError *pErrorHandle,
                                OCITypeIter *pIterOrt);

sword OCITypeArrayByRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                    ub4 arrayLength,
                                    const OCIRef **pTypeReference,
                                    OCIDuration pinDur,
                                    OCITypeGetOpt getOpt,
                                     OCIType **pPinnedType);

oratext* OCITypeName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           const OCIType *pPinnedType,
                            ub4 *pNLen );

ub4 OCITypeAttrs( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                       const OCIType *pPinnedType);

ub4 OCITypeMethods( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCIType *pPinnedType);

ub4 OCITypeElemFlags( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           const OCITypeElem *pTypeElem);

ub4 OCITypeElemLength( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCITypeElem *pTypeElem);

ub2 OCITypeElemCharSetID( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCITypeElem *pTypeElem);

ub2 OCITypeElemCharSetForm( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                 const OCITypeElem *pTypeElem);

sword OCITypeElemParameterizedType( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                         const OCITypeElem *pTypeElem,
                                          OCIType **pTypeStored);

OCITypeCode OCITypeElemExtTypeCode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                         const OCITypeElem *pTypeElem);

ub1 OCITypeElemNumPrec( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             const OCITypeElem *pTypeElem);

sb1 OCITypeElemNumScale( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCITypeElem *pTypeElem);

sword OCITypeByRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCIRef *pTypeReference,
                               OCIDuration pinDur,
                               OCITypeGetOpt getOpt,
                                OCIType **pPinnedType);

sword OCITypeBeginCreate( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                               OCITypeCode typeCode,
                         OCIDuration dur,
                                OCIType **pType);

OCITypeCode OCITypeTypeCode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                  const OCIType *pPinnedType);

sword OCITypeSetCollection( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                                     OCIType *pType,
                           OCIParam *pCollectionElementInfo,
                           ub4 collectionElementCount);

sword OCITypeSetBuiltin( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                                  OCIType *pType,
                        OCIParam *pBuiltinInfo);

sword OCITypeAddAttr( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                               OCIType *pType,
                                      const oratext *pAttrName,
                                      ub4 attrNameLen,
                     OCIParam *pAttrInfo);

sword OCITypeElemType( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCITypeElem *pTypeElem,
                             OCIType **pElemTdo);

oratext* OCITypeSchema( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             const OCIType *pPinnedType,
                              ub4 *pNLen);

oratext* OCITypeElemName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCITypeElem *pTypeElem,
                                ub4 *pNLen);

OCITypeCode OCITypeElemTypeCode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                      const OCITypeElem *pTypeElem);

sword OCITypeAttrByName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCIType *pPinnedType,
                              const oratext *pAttrName, ub4 nLen,
                               OCITypeElem **pTypeElement);

OCITypeCode OCITypeCollTypeCode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                      const OCIType *pPinnedType);

oratext* OCITypeVersion( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCIType *pPinnedType,
                               ub4 *pVLen);

sword OCITypeEndCreate( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                                 OCIType *pType);

sword OCITypeCollSize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCIType *pPinnedType,
                             ub4 *pNumElements);

sword OCITypeCollExtTypeCode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                   const OCIType *pPinnedType,
                                    OCITypeCode *pSqlTypeCode);

ub4 OCITypeMethodOverload( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                const OCIType *pPinnedType,
                                const oratext *pMethodName,
                                ub4 mLen);

oratext *OCITypeMethodName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                 const OCITypeMethod *pMethodType,
                                  ub4 *pNLen);

OCITypeEncap OCITypeMethodEncap( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                      const OCITypeMethod *pMethodType);

OCITypeMethodFlag OCITypeMethodFlags( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                           const OCITypeMethod *pMethodType);

sword OCITypeMethodMap( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             const OCIType *pPinnedType,
                              OCITypeMethod **pMethodType);

sword OCITypeMethodOrder( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCIType *pPinnedType,
                                OCITypeMethod **pMethodType);

ub4 OCITypeMethodParams( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCITypeMethod *pMethodType);

sword OCITypeResult( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCITypeMethod *pMethodType,
                           OCITypeElem **pTypeElement);

sword OCITypeParamByPos( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCITypeMethod *pMethodType,
                              ub4 position,
                               OCITypeElem **pTypeElement);

sword OCITypeParamByName( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCITypeMethod *pMethodType,
                               const oratext *pParamName,
                               ub4 nLen,
                                OCITypeElem **pTypeElement);

sword OCITypeParamPos( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCITypeMethod *pMethodType,
                            const oratext *pParamName,
                            ub4 nLen,
                             ub4 *pPosition,
                             OCITypeElem **pTypeElement);

OCITypeParamMode OCITypeElemParamMode( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                            const OCITypeElem *pTypeElem);

oratext* OCITypeElemDefaultValue( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                        const OCITypeElem *pTypeElem,
                                         ub4 *pDefValueLen);

sword OCITypeVTInit( OCIEnv *pEnvironment, OCIError *pErrorHandle);

sword OCITypeVTInsert( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const oratext *pSchemaName,
                            ub4 schemaNameLen,
                            const oratext *pTypeName,
                            ub4 typeNameLen,
                            const oratext *pUserVersion,
                            ub4 userVersionLen);

sword OCITypeVTSelect( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const oratext *pSchemaName,
                            ub4 schemaNameLen,
                            const oratext *pTypeName,
                            ub4 typeNameLen,
                                       oratext **pUserVersion,
                                       ub4 *pUserVersionLen,
                                       ub2 *pVersion);


sword ortgcty( OCIEnv *pEnvironment, OCIError *pErrorHandle, OCIType *pCollTdo,
                  OCIType **pCollpElemTdo);
# 3151 "/home/db2inst1/sqllib/include/db2ci.h"
sword OCIObjectPin( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         OCIRef *pObjectRef,
                         OCIComplexObject *pCorhdl,
                         OCIPinOpt pinOption,
                         OCIDuration pinDur,
                         OCILockOpt lockOption,
                          void **pObject);

sword OCIObjectUnpin( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           void *pObject);


sword OCIObjectArrayPin( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              OCIRef **pReferencesArray,
                              ub4 arraySize,
                              OCIComplexObject **pCorArray,
                              ub4 corArraySize,
                              OCIPinOpt pinOption,
                              OCIDuration pinDuration,
                              OCILockOpt lockOption,
                               void **pObjectsArray,
                               ub4 *pPos);

sword OCIObjectPinCountReset( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                   void *pObject);

sword OCIObjectLock( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          void *pObject);

sword OCIObjectNew( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCISvcCtx *pServiceContext,
                         OCITypeCode typecode,
                         OCIType *pPinnedType,
                                    void *pTable,
                         OCIDuration dur,
                         int value,
                          void **pInstance);

sword OCIObjectLockNoWait( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                void *pObject);

sword OCIObjectUnmarkByRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                 OCIRef *pObjectReference);

sword OCIObjectFree( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          void *pInstance,
                          ub2 flags);

sword OCIObjectMarkUpdate( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                void *pObject);

sword OCIObjectUnmark( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            void *pObject);

sword OCIObjectFlush( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           void *pObject);

sword OCIObjectMarkDeleteByRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                     OCIRef *pObjectReference);

sword OCIObjectMarkDelete( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                void *pInstance);

sword OCIObjectCopy( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCISvcCtx *pServiceContext,
                          void *pSource,
                          void *pNnullSource,
                          void *pTarget,
                          void *pNullTarget,
                          OCIType *pPinnedType,
                          OCIDuration dur,
                          ub1 option);

sword OCIObjectRefresh( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             void *pObject);

sword OCIObjectMakeObjectRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                   const OCISvcCtx *pServiceContext,
                                   void *pTable,
                                   void **pValues,
                                   ub4 attrlistArrayLen,
                                    OCIRef *pObjectReference);

sword OCIObjectGetTypeRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                void *pInstance,
                                OCIRef *pTypeReference);


sword OCIObjectPinTable( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCISvcCtx *pServiceContext,
                              const oratext *pSchemaName,
                              ub4 sNLen,
                              const oratext *pObjectName,
                              ub4 oNLen,
                              const OCIRef *pScopeObjRef,
                              OCIDuration pinDur,
                               void **pObject);

sword OCIObjectExists( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            void *pInstance,
                             int *pExist);

sword OCIObjectGetObjectRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                  void *pObject,
                                   OCIRef *pObjectReference);

sword OCIObjectGetPrimaryKeyTypeRef( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                          const OCISvcCtx *pServiceContext,
                                          void *pTable,
                                           OCIRef *pTypeReference);

sword OCIObjectGetInd( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            void *pInstance,
                             void **pNullStruct);

sword OCIObjectIsLocked( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              void *pInstance,
                               int *pLock);

sword OCIObjectIsDirty( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             void *pInstance,
                              int *pDirtyStatus);

sword OCIObjectGetProperty( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                 const void *pObj,
                                 OCIObjectPropId propertyId,
                                  void *pProperty,
                                     ub4 *pSize);

sword OCICacheFree( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                   const OCISvcCtx *pServiceContext);

sword OCICacheUnmark( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                     const OCISvcCtx *pServiceContext);
# 3293 "/home/db2inst1/sqllib/include/db2ci.h"
sword OCICacheUnpin( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                    const OCISvcCtx *pServiceContext);

sword OCICacheFlush( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                    const OCISvcCtx *pServiceContext,
                                    void *pContext,
                                    OCIRef *(*get)(void *pContext, ub1 *pLast),
                                     OCIRef **pReference );


sword OCIDurationEnd( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCISvcCtx *pServiceContext,
                            OCIDuration duration);

sword OCIDurationBegin( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                 const OCISvcCtx *pServiceContext,
                             OCIDuration parentDuration,
                              OCIDuration *pDur);
# 3319 "/home/db2inst1/sqllib/include/db2ci.h"
sword OCINumberIntPower( OCIError *pErrorHandle,
                              const OCINumber *pBase,
                              const sword exp,
                               OCINumber *pOutputNum);

sword OCINumberShift( OCIError *pErrorHandle,
                           const OCINumber *pNumber,
                           const sword nDig,
                            OCINumber *pOutputNum);

sword OCINumberToInt( OCIError *pErrorHandle,
                           const OCINumber *pNumber,
                           uword desiredResultLen,
                           uword signFlag,
                            void *pResult);

sword OCINumberLn( OCIError *pErrorHandle,
                        const OCINumber *pNum,
                         OCINumber *pOutputNum);

sword OCINumberNeg( OCIError *pErrorHandle,
                         const OCINumber *pNumber,
                          OCINumber *pOutputNum);

sword OCINumberToText( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                            const oratext *pConvFormat,
                            ub4 convFormatLen,
                            const oratext *pNlsParamsFormat,
                            ub4 nlsParamsFormatLen,
                                ub4 *pBufferSize,
                             oratext *pBuffer);

sword OCINumberFromText( OCIError *pErrorHandle,
                              const oratext *pStr,
                              ub4 strLength,
                              const oratext *pConvFormat,
                              ub4 convFormatLen,
                              const oratext *pNlsParams,
                              ub4 nlsParamsLen,
                               OCINumber *pNumber);

sword OCINumberLog( OCIError *pErrorHandle,
                         const OCINumber *pBase,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);


sword OCINumberToReal( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                            uword resultLen,
                             void *pResult);

sword OCINumberCmp( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          sword *pResult);

sword OCINumberSign( OCIError *pErrorHandle,
                          const OCINumber *pNum,
                           sword *pResult);

sword OCINumberIsZero( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                             int *pResult);

sword OCINumberToRealArray( OCIError *pErrorHandle,
                                 const OCINumber **pNum,
                                 uword numUpperBoundArray,
                                 uword resultLen,
                                  void *pResult);

sword OCINumberFromInt( OCIError *pErrorHandle,
                             const void *pInt,
                             uword pIntLen,
                             uword pIntSignFlag,
                              OCINumber *pNumber);

sword OCINumberFromReal( OCIError *pErrorHandle,
                              const void *pFloatNum,
                              uword floatNumLen,
                               OCINumber *pNum);

sword OCINumberAbs( OCIError *pErrorHandle,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);

sword OCINumberCeil( OCIError *pErrorHandle,
                          const OCINumber *pNum,
                           OCINumber *pOutputNum);

sword OCINumberFloor( OCIError *pErrorHandle,
                           const OCINumber *pNum,
                            OCINumber *pOutputNum);

sword OCINumberSqrt( OCIError *pErrorHandle,
                          const OCINumber *pNum,
                           OCINumber *pOutputNum);

sword OCINumberTrunc( OCIError *pErrorHandle,
                           const OCINumber *pNum,
                           sword numDecDigit,
                            OCINumber *pOutputNum);

sword OCINumberPower( OCIError *pErrorHandle,
                           const OCINumber *pBase,
                           const OCINumber *pExponent,
                            OCINumber *pOutputNum);

sword OCINumberIsInt( OCIError *pErrorHandle,
                           const OCINumber *pNum,
                            int *pResult);

sword OCINumberAssign( OCIError *pErrorHandle,
                            const OCINumber *pNumToAssigned,
                             OCINumber *pNumCopiedInto);

sword OCINumberRound( OCIError *pErrorHandle,
                           const OCINumber *pSourceNum,
                           sword numDecDigit,
                            OCINumber *pOutputNum);

sword OCINumberPrec( OCIError *pErrorHandle,
                          const OCINumber *pNum,
                          sword nDigs,
                           OCINumber *pOutputNum);

sword OCINumberHypSin( OCIError *pErrorHandle,
                            const OCINumber *pNumber,
                             OCINumber *pOutputNum);

sword OCINumberCos( OCIError *pErrorHandle,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);

sword OCINumberArcTan( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                             OCINumber *pOutputNum);

sword OCINumberInc( OCIError *pErrorHandle,
                             OCINumber *pNum);

sword OCINumberDec( OCIError *pErrorHandle,
                             OCINumber *pNum);

sword OCINumberArcCos( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                             OCINumber *pOutputNum);

sword OCINumberHypCos( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                             OCINumber *pOutputNum);

sword OCINumberSin( OCIError *pErrorHandle,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);

sword OCINumberArcSin( OCIError *pErrorHandle,
                            const OCINumber *pNum,
                             OCINumber *pOutputNum);

sword OCINumberTan( OCIError *pErrorHandle,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);


void OCINumberSetZero( OCIError *pErrorHandle,
                             OCINumber *pNum);

void OCINumberSetPi( OCIError *pErrorHandle,
                           OCINumber *pNum);

sword OCINumberMul( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          OCINumber *pOutputNum);

sword OCINumberHypTan( OCIError *pErrorHandle,
                            const OCINumber *pArgument,
                             OCINumber *pOutputNum);

sword OCINumberExp( OCIError *pErrorHandle,
                         const OCINumber *pNum,
                          OCINumber *pOutputNum);

sword OCINumberDiv( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          OCINumber *pOutputNum);


sword OCINumberAdd( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          OCINumber *pOutputNum);


sword OCINumberSub( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          OCINumber *pOutputNum);

sword OCINumberMod( OCIError *pErrorHandle,
                         const OCINumber *pNum1,
                         const OCINumber *pNum2,
                          OCINumber *pOutputNum);

sword OCINumberArcTan2( OCIError *pErrorHandle,
                             const OCINumber *pArgumentX,
                             const OCINumber *pArgumentY,
                              OCINumber *pOutputNum);
# 3539 "/home/db2inst1/sqllib/include/db2ci.h"
sword OCIDateFromText( OCIError *pErrorHandle,
                            const oratext *pDateString,
                            ub4 dateStringLen,
                            const oratext *pConvFormat,
                            ub1 convFormatLen,
                            const oratext *pLanguageName,
                            ub4 languageNameLen,
                             OCIDate *pDateResult);

sword OCIDateZoneToZone( OCIError *pErrorHandle,
                              const OCIDate *pDate1,
                              const oratext *zon1,
                              ub4 zonLen1,
                              const oratext *zon2,
                              ub4 zonLen2,
                               OCIDate *pDate2);

sword OCIDateSysDate( OCIError *pErrorHandle,
                            OCIDate *pSysDate);

sword OCIDateCompare( OCIError *pErrorHandle,
                           const OCIDate *pDate1,
                           const OCIDate *pDate2,
                            sword *pCompResult);

sword OCIDateAddMonths( OCIError *pErrorHandle,
                             const OCIDate *pInputDate,
                             sb4 numMonths,
                              OCIDate *pDateResult);

sword OCIDateCheck( OCIError *pErrorHandle,
                         const OCIDate *pInputDate,
                          uword *pValidDate);

sword OCIDateAssign( OCIError *pErrorHandle,
                          const OCIDate *pSourceDate,
                           OCIDate *pTargetDate);

sword OCIDateToText( OCIError *pErrorHandle,
                          const OCIDate *pInputDate,
                          const oratext *pConvFormat,
                          ub1 convFormatLen,
                          const oratext *pLanguageName,
                          ub4 languageNameLen,
                              ub4 *pBufferSize,
                           oratext *pBuffer);

sword OCIDateNextDay( OCIError *pErrorHandle,
                           const OCIDate *pInputDate,
                           const oratext *pFirstDay,
                           ub4 firstDayLen,
                            OCIDate *pNextSay);

sword OCIDateDaysBetween( OCIError *pErrorHandle,
                               const OCIDate *pInputDate1,
                               const OCIDate *pInputDate2,
                                sb4 *pNumDays);

sword OCIDateAddDays( OCIError *pErrorHandle,
                           const OCIDate *pInputDate,
                           sb4 numDays,
                            OCIDate *pDateResult);

sword OCIDateLastDay( OCIError *pErrorHandle,
                           const OCIDate *pInputDate,
                            OCIDate *pLastDay);







sword OCIStringAllocSize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                               const OCIString *pStr,
                                ub4 *pAllocSize);

ub4 OCIStringSize( OCIEnv *pEnvironment,
                        const OCIString *pVs);

oratext *OCIStringPtr( OCIEnv *pEnvironment,
                            const OCIString *pVs);

sword OCIStringAssignText( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                const oratext *pRightString,
                                ub4 rightStringLen,
                                    OCIString **pLeftString);

sword OCIStringAssign( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCIString *pRightString,
                                OCIString **pLeftString);

sword OCIStringResize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            ub4 newSize,
                                OCIString **pStr);




sword OCIRawAssignBytes( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const ub1 *pRightString,
                              ub4 rightStringLen,
                                  OCIRaw **pLeftString);

sword OCIRawAllocSize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCIRaw *pRaw,
                             ub4 *pAllocSize);

sword OCIRawAssignRaw( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            const OCIRaw *pRightString,
                                OCIRaw **pLeftString);

ub1 *OCIRawPtr( OCIEnv *pEnvironment,
                     const OCIRaw *pRaw);

sword OCIRawResize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         ub4 newSize,
                         OCIRaw **pRaw);

ub4 OCIRawSize( OCIEnv * EnvironmentHandle,
                     const OCIRaw * Raw );




void OCIRefClear( OCIEnv *pEnvironment,
                           OCIRef *pReference);

sword OCIRefAssign( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCIRef *pSourceRef,
                             OCIRef **pTargetRef);

int OCIRefIsEqual( OCIEnv *pEnvironment,
                            const OCIRef *pReference1,
                            const OCIRef *pReference2);

int OCIRefIsNull( OCIEnv *pEnvironment,
                           const OCIRef *pReference);

ub4 OCIRefHexSize( OCIEnv *pEnvironment,
                        const OCIRef *pReference);

sword OCIRefFromHex( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCISvcCtx *pServiceContext,
                          const oratext *pHexStr,
                          ub4 Len,
                              OCIRef **pReference);

sword OCIRefToHex( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                        const OCIRef *pReference,
                         oratext *pHex,
                            ub4 *pHexLen);






sword OCICollGetElemArray( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                                const OCIColl *pCollection,
                                sb4 indexElement,
                                 int *pElemExists,
                                 void **pTypeElement,
                                 void **pElementIndicators,
                                    uword *pUpperBoundElemsIndicator);

sword OCICollGetElem( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           const OCIColl *pCollection,
                           sb4 indexElement,
                            int *pElemExists,
                            void **pTypeElement,
                            void **pElementIndicators);

sword OCICollAssignElem( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              sb4 indexElement,
                              const void *pTypeElement,
                              const void *pElementIndicators,
                                  OCIColl *pCollection);

sword OCICollAppend( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const void *pTypeElement,
                          const void *pElementIndicators,
                              OCIColl *pCollection);

sword OCICollAssign( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCIColl *pSourceCollection,
                           OCIColl *pTargetCollection);

sword OCICollTrim( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                        sb4 elementsTrimNum,
                            OCIColl *pCollection);

sword OCICollIsLocator( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                             const OCIColl *pCollection,
                              int *pResult);

sword OCICollSize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                        const OCIColl *pCollection,
                         sb4 *pSize);

sb4 OCICollMax( OCIEnv *pEnvironment,
                     const OCIColl *pCollection);




sword OCIIterInit( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                        const OCIColl *pCollection,
                            OCIIter *pItr);

sword OCIIterNext( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            OCIIter *pItr,
                         void **pElement,
                         void **pElementIndicators,
                         int *pEndOfCollection);

sword OCIIterPrev( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                            OCIIter *pItr,
                         void **pElement,
                         void **pElementIndicators,
                         int *pBeginningOfCollection);

sword OCIIterCreate( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCIColl *pCollection,
                           OCIIter **pCollectionItr);

sword OCIIterGetCurrent( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              const OCIIter *pItr,
                               void **pElement,
                               void **pElementIndicators);

sword OCIIterDelete( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                              OCIIter **pCollectionItr);




sword OCITableFirst( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                          const OCITable *pScannedTable,
                           sb4 *pIndexElement );

sword OCITableSize( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCITable *pNestedTable,
                          sb4 *pSize);

sword OCITableLast( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCITable *pScannedTable,
                          sb4 *pIndexElement );

sword OCITablePrev( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         sb4 index,
                         const OCITable *pScannedTable,
                          sb4 *pRevIndex,
                          int *pIndexExists);

sword OCITableExists( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           const OCITable *pNestedTable,
                           sb4 indexElement,
                            int *pElementExists);

sword OCITableDelete( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           sb4 indexElement,
                           OCITable *pScannedTable);

sword OCITableNext( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         sb4 indexElement,
                         const OCITable *pScannedTable,
                          sb4 *pNextIndex,
                          int *pIndexExists);



void *pOciepacm(OCIExtProcContext *pWithContext, size_t amount);
size_t ocieperr(OCIExtProcContext *pWithContext, int errorNumber);
size_t ociepmsg(OCIExtProcContext *pWithContext, int errorNumber,
                oratext *pErrorMessage, size_t len);
sword ociepgoe(OCIExtProcContext *pWithContext, OCIEnv **pEnvh,
               OCISvcCtx **pSvch, OCIError **pErrh);



sword ocon (struct cdaDef *pLda);
sword obindps(struct cdaDef *pCursor, ub1 opcode,
               OraText *pSqlvar,
               sb4 sqlvl, ub1 *pPvctx,
               sb4 progvl,
               sword ftype, sword scale,
               sb2 *pIndp, ub2 *pALen,
               ub2 *pArcode,
               sb4 pvSkip, sb4 indSkip,
               sb4 alenSkip, sb4 rcSkip,
               ub4 maxsiz, ub4 *pCursiz,
               oratext *pConvFormat,
               sb4 fmtl, sword fmtt);

sword obndrn( struct cdaDef * pCursor,
              sword sqlvn,
              ub1 * pPvctx,
              sword progvl,
              sword ftype,
              sword scale,
              sb2 * pIndp,
              text * pConvFormat,
              sword fmtl,
              sword fmtt );







sword ocan (struct cdaDef *pCursor);
sword ocom (struct cdaDef *pLda);
sword ocof (struct cdaDef *pLda);




 sword odescr(struct cdaDef *pCursor,
              sword pos, sb4 *pDbsize,
              sb2 *pDbtype, sb1 *pCbuf,
              sb4 *pCbufl, sb4 *pDsize,
              sb2 *pRec, sb2 *pScale, sb2 *pNullok);

 sword odefinps(struct cdaDef *pCursor,
                ub1 opcode, sword pos,
                ub1 *pBufctx, sb4 bufl,
                sword ftype, sword scale,
                sb2 *pIndp, oratext *pConvFormat,
                sb4 fmtl, sword fmtt,
                ub2 *pRLen, ub2 *pRcode,
                sb4 pvSkip, sb4 indSkip,
                sb4 alenSkip, sb4 rcSkip);




sword oclose(struct cdaDef *pCursor);
sword obreak(struct cdaDef *pLda);

sword osetpi (struct cdaDef *pCursor,
                 ub1 piece, void *pBufp,
                 ub4 *pLenp);

void sqlld2 (struct cdaDef *pLda,
                 OraText *pCname,
                 sb4 *pCnLen);

void sqllda (struct cdaDef *pLda);

sword odessp(struct cdaDef *pCursor, OraText *pObjnam, size_t onLen,
              ub1 *pRsv1, size_t rsv1ln, ub1 *pRsv2, size_t rsv2ln,
              ub2 *pOvrld, ub2 *pPos, ub2 *pLevel, OraText **pArgnam,
              ub2 *pArnLen, ub2 *pDtype, ub1 *pDefsup, ub1 *pMode,
              ub4 *pDtsiz, sb2 *pRec, sb2 *pScale, ub1 *pRadix,
              ub4 *pSpare, ub4 *pArrsiz);


sword oerhms (struct cdaDef *pLda,
                 sb2 rcode,
                 oratext *pBuffer,
                 sword bufsiz);

sword oermsg (sb2 rcode, oratext *pBuffer);
sword oparse (struct cdaDef *pCursor,
                 OraText *pSqlstm, sb4 sqlLen,
                 sword defflg, ub4 lngflg);

sword orol (struct cdaDef *pLda);

sword oexn (struct cdaDef *pCursor, sword iters, sword rowOff);
sword ofen (struct cdaDef *pCursor, sword nRows);
sword ofetch (struct cdaDef *pCursor);
sword oflng (struct cdaDef *pCursor, sword pos, ub1 *pBuffer,
                 sb4 bufl, sword dtype, ub4 *pRetl, sb4 offset);
sword ogetpi (struct cdaDef *pCursor, ub1 *pPiecep, void **pCtxpp,
                 ub4 *pIterp, ub4 *pIndexp);
sword oopt (struct cdaDef *pCursor, sword rbopt, sword waitopt);
sword opinit (ub4 mode);
sword oexec (struct cdaDef *pCursor);
sword oexfet (struct cdaDef *pCursor, ub4 nRows,
                 sword cancel, sword exact);



sword orlon (struct cdaDef *pLda, ub1 *pHda,
                 OraText *pUid, sword uidl,
                 OraText *pSwd, sword pswdl,
                 sword audit);

sword onbtst (struct cdaDef *pLda);

sword olog (struct cdaDef *pLda, ub1 *pHda,
                 OraText *pUid, sword uidl,
                 OraText *pSwd, sword pswdl,
                 OraText *pConn, sword connl,
                 ub4 mode);

sword ologof (struct cdaDef *pLda);

sword ognfd (struct cdaDef *pLda, void *pFdp);

sword olon (struct cdaDef *pLda,
                 OraText *pUid,
                 sword uidl,
                 OraText *pSwd,
                 sword pswdl,
                 sword audit);

sword oname (struct cdaDef *pCursor, sword pos,
                 sb1 *pTbuf, sb2 *pTbufl,
                 sb1 *pBuffer, sb2 *pBufl);


sword osql3 (struct cdaDef *pCda,
                 OraText *pSqlstm,
                 sword sqlLen);

sword odsc (struct cdaDef *pCursor,
                 sword pos, sb2 *pDbsize,
                 sb2 *pFsize, sb2 *pRcode,
                 sb2 *pDtype, sb1 *pBuffer,
                 sb2 *pBufl, sb2 *pDsize);

sword onbset (struct cdaDef *pLda);

sword oopen (struct cdaDef *pCursor,
                 struct cdaDef *pLda,
                 OraText *pDbn, sword dbnl,
                 sword arsize,
                 OraText *pUid, sword uidl);
sword onbclr (struct cdaDef *pLda);
# 3980 "/home/db2inst1/sqllib/include/db2ci.h"
nzerror nzteOpenPersona( nzctx *pOssContext,
                                  nzttPersona *pPersona);


nzerror nztwRetrievePersonaCopy( nzctx *pOssContext,
                                      nzttWallet *pWallet,
                                      ub4 index,
                                       nzttPersona **pPersonaFound);

nzerror nzteClosePersona( nzctx *pOssContext,
                                   nzttPersona *pPersona);

nzerror nztific_FreeIdentityContent( nzctx *pOssContext,
                                              nzttIdentity *pIdentity);

nzerror nztSign( nzctx *pOssContext,
                      nzttPersona *pPersona,
                      nzttces signatureState,
                      ub4 inputPartLen,
                      ub1 *pInputPart,
                          nzttBufferBlock *pTduBuffer);

nzerror nztwOpenWallet( nzctx *pOssContext,
                             ub4 wrlLen,
                             text *pWrl,
                             ub4 passwordLen,
                             text *pPassword,
                                 nzttWallet *pWallet);

nzerror nztwCloseWallet( nzctx *pOssContext,
                                  nzttWallet *pWallet);

nzerror nztwRetrievePersonaCopyByName( nzctx *pOssContext,
                                            nzttWallet *pWallet,
                                            char *pPersonaName,
                                             nzttPersona **pPersonaFound);
nzerror nzteDestroyPersona( nzctx *pOssContext,
                                     nzttPersona **pPersona);


nzerror nzteX509CreatePersona( nzctx *pOssContext,
                                    ub1 *pCertificate,
                                    ub4 certificateLen,
                                     nzttPersona **pPersona);

nzerror nztiCreateIdentity( nzctx *pOssContext,
                                 nzttVersion identityType,
                                 nzttIdentityDesc *pIdentityDescription,
                                     nzttIdentity ** pPersonaIdentity);

nzerror nztiAbortIdentity( nzctx *pOssContext,
                                    nzttIdentity **pIdentity);


nzerror nztVerify( nzctx *pOssContext,
                        nzttPersona *pPersona,
                        nzttces verificationState,
                        ub4 tduLen,
                        ub1 tdu,
                            nzttBufferBlock *pMessageExtracted,
                         int *pVerified,
                         int *pValidated,
                         nzttIdentity ** partyIdentity);

nzerror nztValidate( nzctx *pOssContext,
                          nzttPersona *pPersona,
                          nzttIdentity *pIdentity,
                           int *pValidated);

nzerror nztsd_SignDetached( nzctx *pOssContext,
                                 nzttPersona *pPersona,
                                 nzttces signatureState,
                                 ub4 inputPartLen,
                                 ub1 *pInputPart,
                                     nzttBufferBlock *pTduBuffer);

nzerror nztrn_RandomNumber( nzctx *pOssContext,
                                 nzttPersona *pPersona,
                                  uword *pNum);

nzerror nztbbInitBlock( nzctx *pOssContext,
                                 nzttBufferBlock *pBuffer );

nzerror nztbbReuseBlock( nzctx *pOssContext,
                                  nzttBufferBlock *pBuffer );

nzerror nztved_VerifyDetached( nzctx *pOssContext,
                                    nzttPersona *pPersona,
                                    nzttces verificationState,
                                    ub4 dataLen,
                                    ub1 *pData,
                                    ub4 tduLen,
                                    ub1 *pTdu,
                                     int *pVerifiedSignature,
                                     int *pValidatedIdentity,
                                     nzttIdentity **pPartyIdentity);

nzerror nztkec_PKEncrypt( nzctx *pOssContext,
                               nzttPersona *pPersona,
                               ub4 recipientsNum,
                               nzttIdentity *pRecipientsList,
                               nzttces encryptionState,
                               ub4 inputPartLen,
                               ub1 *pInputPart,
                                   nzttBufferBlock *pTduBuffer);

nzerror nztxkec_PKEncryptExpansion( nzctx *pOssContext,
                                         nzttPersona *pPersona,
                                         ub4 recipientsNum,
                                         ub4 inputLen,
                                          ub4 *pBufferLen);

nzerror nztkdc_PKDecrypt( nzctx *pOssContext,
                               nzttPersona *pPersona,
                               nzttces encryptionState,
                               ub4 inputPartLen,
                               ub1 *pInputPart,
                                   nzttBufferBlock *pTduBuffer);

nzerror nztHash( nzctx *pOssContext,
                      nzttPersona *pPersona,
                      nzttces encryptionState,
                      ub4 inputLen,
                      ub1 *pInput,
                          nzttBufferBlock *pTduBuffer );

nzerror nztSeedRandom( nzctx *pOssContext,
                            nzttPersona *pPersona,
                            ub4 seedLen,
                            ub1 *pSeed);

nzerror nztrb_RandomBytes( nzctx *pOssContext,
                                nzttPersona *pPersona,
                                ub4 bufferLen,
                                    nzttBufferBlock *pBuffer );

nzerror nztbbSizeBlock( nzctx *pOssContext,
                             ub4 bufferMinLen,
                                 nzttBufferBlock *pBuffer );

nzerror nztbbGrowBlock( nzctx *pOssContext,
                             ub4 increaseNum,
                                 nzttBufferBlock *pBuffer );

nzerror nztGetCertChain( nzctx *pOssContext,
                              nzttWallet *pWallet);

nzerror nztCompareDN( nzctx *pOssContext,
                           ub1 *pDistinguishedName1,
                           ub4 distName1Len,
                           ub1 *pDistinguishedName2,
                           ub4 distName2Len,
                            int *pResult);

nzerror nztbbPurgeBlock( nzctx *pOssContext,
                                  nzttBufferBlock *pBuffer );

nzerror nztbbSetBlock( nzctx *pOssContext,
                            uword setFlag,
                            ub4 bufferLen,
                            ub4 usedLen,
                            ub1 *pBuffer,
                                nzttBufferBlock *pBlockBuffer );

nzerror nztiGetSecInfo( nzctx *pOssContext,
                             nzttPersona *pPersona,
                              text **pDistinguishedName,
                              ub4 *pDistinguishedNameLen,
                              text **pIssuerName,
                              ub4 *pIssuerNameLen,
                              ub1 **pCertificateHash,
                              ub4 *pHashLen);

nzerror nztiGetDName( nzctx *pOssContext,
                           nzttIdentity *pIdentity,
                            text **pDnName,
                            ub4 *pDnLen);

nzerror nztiGetIssuerName( nzctx *pOssContext,
                                nzttIdentity *pIdentity,
                                 text **pIssuerName,
                                 ub4 *pIssuerNameLen);

nzerror nztgch_GetCertHash( nzctx *pOssContext,
                                 nzttIdentity *pIdentity,
                                  ub1 **pCertHashBuffer,
                                  ub4 *pCertHashBufferLen);

nzerror nztdbuf_DestroyBuf( nzctx *pOssContext,
                                 void **pBuffer);
# 4195 "/home/db2inst1/sqllib/include/db2ci.h"
nzerror nzteCreatePersona( nzctx *pOssContext,
                                nzttVersion identityType,
                                nzttCipherType cipherType,
                                nzttPersonaDesc *pPersonaDescription,
                                 nzttPersona **pPersona);

nzerror nztiStoreTrustedIdentity( nzctx *pOssContext,
                                        nzttIdentity **pTrustedIdentity,
                                           nzttPersona *pPersona);

nzerror nztCheckValidity( nzctx *pOssContext,
                               ub4 startTime,
                               ub4 endTime);

nzerror nztwCreateWallet( nzctx *pOssContext,
                               ub4 wrlLen,
                               text *pWrl,
                               ub4 passwordLen,
                               text *pPassword,
                                   nzttWallet *pWallet);

nzerror nztwDestroyWallet( nzctx *pOssContext,
                                ub4 wrlLen,
                                text *pWrl,
                                ub4 passwordLen,
                                text *pPassword);

nzerror nzteStorePersona( nzctx *pOssContext,
                                   nzttPersona **pPersona,
                                   nzttWallet *pWallet);

nzerror nzteRemovePersona( nzctx *pOssContext,
                                 nzttPersona **pPersona);

nzerror nzteSetProtection( nzctx *pOssContext,
                                nzttPersona *pPersona,
                                nzttcef ceFunc,
                                nztttdufmt tduFormat,
                                nzttProtInfo *pRotectionInfo);

nzerror nzteGetProtection( nzctx *pOssContext,
                                nzttPersona *pPersona,
                                nzttcef ceFunc,
                                 nztttdufmt *pTduFormat,
                                 nzttProtInfo *pRotectionInfo);

nzerror nztiRemoveIdentity( nzctx *pOssContext,
                                     nzttIdentity **pIdentity);

nzerror nztifdn( nzctx *pOssctx,
                      ub4 Len,
                      text *pDistinguishedName,
                       nzttIdentity **pCreatedIdentity);

nzerror nztxSignExpansion( nzctx *pOssContext,
                                nzttPersona *pPersona,
                                ub4 inputLen,
                                 ub4 *pTduLen);

nzerror nztxsd_SignDetachedExpansion( nzctx *pOssContext,
                                           nzttPersona *pPersona,
                                           ub4 inputLen,
                                            ub4 *pSignatureBufferLen);

nzerror nztEncrypt( nzctx *pOssContext,
                         nzttPersona *pPersona,
                         nzttces state,
                         ub4 inputPartLen,
                         ub1 *pInputPart,
                             nzttBufferBlock *pTduBuffer );

nzerror nztxEncryptExpansion( nzctx *pOssContext,
                                   nzttPersona *pPersona,
                                   ub4 inputPartLen,
                                    ub4 *pTduLen);

nzerror nztDecrypt( nzctx *pOssContext,
                         nzttPersona *pPersona,
                         nzttces decryptionState,
                         ub4 inputPartLen,
                         ub1 *pInputPart,
                             nzttBufferBlock *pBuffer );

nzerror nztEnvelope( nzctx *pOssContext,
                          nzttPersona *pPersona,
                          ub4 recipientsListLen,
                          nzttIdentity *pRecipientsList,
                          nzttces encryptionState,
                          ub4 inputPartLen,
                          ub1 *pInputPart,
                              nzttBufferBlock *pTduBuffer );

nzerror nztiae_IsAuthEnabled( nzctx *pOssContext,
                                   ub2 cipherSuite,
                                    int *pAuthEnabled);

nzerror nztiee_IsEncrEnabled( nzctx *pOssContext,
                                   ub2 cipherSuite,
                                    int *pEncrEnabled);

nzerror nztihe_IsHashEnabled( nzctx *pOssContext,
                                   ub2 cipherSuite,
                                    int *pHashEnabled);

nzerror nztDeEnvelope( nzctx *pOssContext,
                            nzttPersona *pPersona,
                            nzttces State,
                            ub4 inputPartLen,
                            ub1 *pInputPart,
                             nzttBufferBlock *pBuffer,
                             int *pVerified,
                             int *pValidated,
                             nzttIdentity **pSenderIdentity);

nzerror nztKeyedHash( nzctx *pOssContext,
                           nzttPersona *pPersona,
                           nzttces hashState,
                           ub4 inputLen,
                           ub1 *pInput,
                               nzttBufferBlock *pBuffer );

nzerror nztxKeyedHashExpansion( nzctx *pOssContext,
                                     nzttPersona *pPersona,
                                     ub4 inputLen,
                                      ub4 *pTduLen);


nzerror nztxHashExpansion( nzctx *pOssContext,
                                nzttPersona *pPersona,
                                ub4 inputLen,
                                 ub4 *pTduLen);


nzerror nztGetSubjectName( nzctx *pOssContext,
                          nzttIdentity *pIdentity,
                          ub1 **pSubjectname,
                          ub4 *pSubjectnameLen);

nzerror nztGetPublicKey( nzctx *pOssContext,
                        nzttIdentity *pIdentity,
                        ub1 **pPubKey,
                        ub4 *pPubKeyLen);

nzerror nztGetBase64Cert( nzctx *pOssContext,
                         nzttIdentity *pIdentity,
                         ub1 **pB64cert,
                         ub4 *pB64certLen);

nzerror nztGetSerialNumber( nzctx *pOssContext,
                           nzttIdentity *pIdentity,
                           ub1 **pSerialNum,
                           ub4 *pSerialNumLen);

nzerror nztGetIssuerName( nzctx *pOssContext,
                         nzttIdentity *pIdentity,
                         ub1 **pIssuername,
                         ub4 *pIssuernameLen);

nzerror nztGetValidDate( nzctx *pOssContext,
                        nzttIdentity *pIdentity,
                        ub4 *pStartDate,
                        ub4 *pEndDate );

nzerror nztSetAppDefaultLocation( nzctx *pOssContext,
                                 text *,
                                 size_t);

nzerror nztSearchNZDefault( nzctx *pOssContext,
                           int *pSearch);

nzerror nztSetLightWeight( nzctx *pOssContext,
                          int flag);

nzerror nztGetVersion( nzctx *pOssContext,
                      nzttIdentity *pIdentity,
                      nzstrc *pVerStr );

nzerror nztGenericDestroy( nzctx *pOssContext,
                          ub1 **pVar);






sword OCITerminate( ub4 mode);

sword OCIInitialize ( ub4 mode, void *pCtxp,
                       OCICallbackMalloc malocfp,
                       OCICallbackRaloc ralocfp,
                       OCICallbackFree mfreefp );


sword OCIEnvCreate ( OCIEnv **pEnvp,
                            ub4 mode,
                            void *pCtxp,
                       OCICallbackMalloc malocfp,
                       OCICallbackRaloc ralocfp,
                       OCICallbackFree mfreefp,
                            size_t xtramemSize,
                             void **pUsrmempp);

sword OCIEnvNlsCreate ( OCIEnv **pEnvp, ub4 mode, void *pCtxp,
                 void *(*pMalocfp)(void *pCtxp, size_t size),
                 void *(*pRalocfp)(void *pCtxp, void *pMemptr, size_t newSize),
                 void (*pMfreefp)(void *pCtxp, void *pMemptr),
                 size_t xtramemSize, void **pUsrmempp,
                 ub2 charset, ub2 ncharset);

sword OCIFEnvCreate ( OCIEnv **pEnvp, ub4 mode, void *pCtxp,
                 void *(*pMalocfp)(void *pCtxp, size_t size),
                 void *(*pRalocfp)(void *pCtxp, void *pMemptr, size_t newSize),
                 void (*pMfreefp)(void *pCtxp, void *pMemptr),
                 size_t xtramemSize, void **pUsrmempp, void *pFupg);

sword OCIHandleAlloc( const void *pParenth,
                               void **pHndlpp,
                              const ub4 type,
                              const size_t xtramemSize,
                               void **pUsrmempp);

sword OCIArrayDescriptorAlloc(const void *pParenth, void **pDescpp,
                           const ub4 type, ub4 arraySize,
                           const size_t xtramemSize, void **pUsrmempp);

sword OCIDescriptorFree(void *pDescp, const ub4 type);

sword OCIArrayDescriptorFree(void **pDescp, const ub4 type);

sword OCIEnvInit ( OCIEnv **pEnvp,
                          ub4 mode,
                          size_t xtramemSize,
                           void **pUsrmempp);

sword OCIServerAttach ( OCIServer *pSrvhp,
                                    OCIError *pErrorHandle,
                                const OraText *pDblink,
                                sb4 dblinkLen,
                                ub4 mode);

sword OCIServerDetach ( OCIServer *pSrvhp,
                                    OCIError *pErrorHandle,
                                ub4 mode);

sword OCISessionBegin ( OCISvcCtx *pServiceContexthp,
                                OCIError *pErrorHandle,
                                    OCISession *pUsrhp,
                                ub4 credt,
                                ub4 mode);


sword OCIHandleFree( void *pHndlp,
                            const ub4 type);


sword OCIDescriptorAlloc(const void *pParenth, void **pDescpp,
                           const ub4 type, const size_t xtramemSize,
                           void **pUsrmempp);

sword OCISessionEnd ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                               OCISession *pUsrhp,
                               ub4 mode);

sword OCILogon2 ( OCIEnv *pEnvironment,
                             OCIError *pErrorHandle,
                          OCISvcCtx **pSvchp,
                        const OraText *pUserName,
                        ub4 unameLen,
                        const OraText *pPassword,
                        ub4 passwdLen,
                        const OraText *pDbname,
                        ub4 dbnameLen,
                        ub4 mode);

sword OCILogoff ( OCISvcCtx *pServiceContexthp,
                             OCIError *pErrorHandle);

sword OCILogon ( OCIEnv *pEnvironment,
                            OCIError *pErrorHandle,
                         OCISvcCtx **pSvchp,
                        const OraText *pUserName,
                        ub4 unameLen,
                        const OraText *pPassword,
                        ub4 passwdLen,
                        const OraText *pDbname,
                        ub4 dbnameLen);


sword OCIStmtPrepare2 ( OCISvcCtx *pServiceContexthp,
                              OCIStmt **pStmtp,
                             OCIError *pErrorHandle,
                             const OraText *pStmt,
                             ub4 stmtLen,
                             const OraText *pKey,
                             ub4 keyLen,
                             ub4 language,
                             ub4 mode);

sword OCIStmtRelease ( OCIStmt *pStmtp,
                                OCIError *pErrorHandle,
                            const OraText *pKey,
                             ub4 keyLen,
                             ub4 mode);

sword OCIPasswordChange ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                                   const OraText *pUserName,
                                   ub4 usernmLen,
                                   const OraText *pOpasswd,
                                   ub4 opasswdLen,
                                   const OraText *pNpasswd,
                                   ub4 npasswdLen,
                                   ub4 mode);

sword OCIBindArrayOfStruct ( OCIBind *pBindp,
                                      OCIError *pErrorHandle,
                                      ub4 pvskip,
                                      ub4 indskip,
                                      ub4 alskip,
                                      ub4 rcskip);

sword OCIStmtGetPieceInfo ( OCIStmt *pStmtp,
                                      OCIError *pErrorHandle,
                                      void **pHndlpp,
                                      ub4 *pTypep,
                                      ub1 *pInOutp,
                                      ub4 *pIterp,
                                      ub4 *pIdxp,
                                      ub1 *pPiecep);

sword OCIBindByPos ( OCIStmt *pStmtp,
                                 OCIBind **pBindp,
                                 OCIError *pErrorHandle,
                             ub4 position,
                                 void *pValuep,
                             sb4 valueSize,
                             ub2 dty,
                                 void *pIndp,
                                 ub2 *pAlenp,
                              ub2 *pRcodep,
                             ub4 maxarrLen,
                                 ub4 *pCurelep,
                             ub4 mode);

sword OCIBindByName ( OCIStmt *pStmtp,
                                   OCIBind **pBindp,
                                   OCIError *pErrorHandle,
                               const OraText *placeholder,
                               sb4 placehLen,
                                   void *pValuep,
                               sb4 valueSize,
                               ub2 dty,
                                   void *pIndp,
                                   ub2 *pAlenp,
                                ub2 *pRcodep,
                               ub4 maxarrLen,
                                   ub4 *pCurelep,
                               ub4 mode);

sword OCIStmtExecute ( OCISvcCtx *pServiceContexthp,
                                   OCIStmt *pStmtp,
                                   OCIError *pErrorHandle,
                               ub4 iters,
                               ub4 rowoff,
                               const OCISnapshot *pSnapIn,
                                OCISnapshot *pSnapOut,
                               ub4 mode);

sword OCIStmtPrepare ( OCIStmt *pStmtp,
                                OCIError *pErrorHandle,
                                const OraText *pStmt,
                                ub4 stmtLen,
                                ub4 language,
                                ub4 mode);

sword OCIBindObject ( OCIBind *pBindp,
                                  OCIError *pErrorHandle,
                              const OCIType *pType,
                                  void **pgvpp,
                                  ub4 *pPvszsp,
                                  void **pIndpp,
                                  ub4 *pIndszp);

sword OCIBindDynamic ( OCIBind *pBindp,
                                    OCIError *pErrorHandle,
                                void *pIctxp,
                                OCICallbackInBind icbfp,
                          void *pOctxp,
                          OCICallbackOutBind ocbfp);

sword OCIStmtSetPieceInfo ( void *pHndlp, ub4 type,
                                     OCIError *pErrorHandle,
                                      const void *pBufp,
                                         ub4 *pAlenp,
                                     ub1 piece,
                                         const void *pIndp,
                                         ub2 *pRcodep);

sword OCIDefineArrayOfStruct (OCIDefine *pDefnp, OCIError *pErrorHandle, ub4 pvskip,
                                 ub4 indskip, ub4 rlskip, ub4 rcskip);

sword OCIDefineObject (OCIDefine *pDefnp, OCIError *pErrorHandle,
                          const OCIType *pType, void **pPgvpp,
                          ub4 *pPvszsp, void **pIndpp, ub4 *pIndszp);

sword OCIDefineByPos ( OCIStmt *pStmtp,
                                   OCIDefine **pDefnp,
                               OCIError *pErrorHandle,
                               ub4 position,
                                   void *pValuep,
                               sb4 valueSize,
                               ub2 dty,
                               void *pIndp,
                                   ub2 *pRlenp,
                                ub2 *pRcodep,
                               ub4 mode);

sword OCIDefineDynamic (OCIDefine *pDefnp, OCIError *pErrorHandle, void *pOctxp,
                            OCICallbackDefine ocbfp);

sword OCIRowidToChar (OCIRowid *pRowidDesc, OraText *pOutbfp, ub2 *pOutbflp,
                         OCIError *pErrorHandle);


sword OCIStmtFetch (OCIStmt *pStmtp, OCIError *pErrorHandle, ub4 nRows,
                        ub2 orientation, ub4 mode);

sword OCIStmtFetch2 (OCIStmt *pStmtp, OCIError *pErrorHandle, ub4 nRows,
                        ub2 orientation, sb4 scrollOffset, ub4 mode);

sword OCIStmtGetBindInfo (OCIStmt *pStmtp, OCIError *pErrorHandle, ub4 size,
                              ub4 startloc,
                              sb4 *pFound, OraText *pBvnp[], ub1 bvnl[],
                              OraText *pInvp[], ub1 inpl[], ub1 dupl[],
                              OCIBind **pHndl);

sword OCIDescribeAny ( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                         void *pObjptr,
                         ub4 objnmLen, ub1 objptrTyp, ub1 infoLevel,
                         ub1 objtyp, OCIDescribe *pDschp);

sword OCIParamGet (const void *pHndlp, ub4 htype,
                               OCIError *pErrorHandle,
                     void **pParmdpp, ub4 pos);

sword OCIParamSet(void *pHdlp, ub4 htyp, OCIError *pErrorHandle, const void *pDscp,
                    ub4 dtyp, ub4 pos);

sword OCITransStart ( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle,
                        uword timeout, ub4 flags);

sword OCITransDetach ( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle, ub4 flags);

sword OCITransMultiPrepare ( OCISvcCtx *pServiceContexthp, ub4 numBranches,
                              OCITrans **pTxns, OCIError **pErrorHandle);

sword OCITransForget ( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle, ub4 flags);

sword OCIErrorGet ( void *pHndlp,
                             ub4 recordno,
                              OraText *pSqlstate,
                              sb4 *pErrcodep,
                              OraText *pBufp,
                             ub4 bufsiz,
                             ub4 type);

sword OCITransCommit ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle, ub4 flags);

sword OCITransRollback ( OCISvcCtx *pServiceContexthp,
                                     OCIError *pErrorHandle, ub4 flags);

sword OCITransPrepare ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle, ub4 flags);



sword OCILobCreateTemporary(OCISvcCtx *pSvchp,
                            OCIError *pErrorHandle,
                            OCILobLocator *pLocp,
                            ub2 csid,
                            ub1 csfrm,
                            ub1 lobtype,
                            int cache,
                            OCIDuration duration);

sword OCILobCopy ( OCISvcCtx *pServiceContexthp, OCIError *pErrorHandle, OCILobLocator *pDstLocp,
                    OCILobLocator *pSrcLocp, ub4 amount, ub4 dstOffset,
                    ub4 srcOffset);

sword OCILobFileClose ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                         OCILobLocator *pFilep);

sword OCILobFileCloseAll ( OCISvcCtx *pServiceContexthp,
                            OCIError *pErrorHandle);

sword OCILobFileExists ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCILobLocator *pFilep,
                          int *pFlag);

sword OCILobFileGetName ( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           const OCILobLocator *pFilep,
                           OraText *pDirAlias, ub2 *pDLength,
                           OraText *pFilename, ub2 *pFLength);


sword OCILobFileOpen ( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCILobLocator *pFilep,
                        ub1 mode);

sword OCILobAssign ( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                      const OCILobLocator *pSrcLocp,
                      OCILobLocator **pDstLocpp);

sword OCILobFileIsOpen ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCILobLocator *pFilep,
                          int *pFlag);

sword OCILobCharSetForm ( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                           const OCILobLocator *pLocp, ub1 *pCsfrm);

sword OCILobCharSetId ( OCIEnv *pEnvironment, OCIError *pErrorHandle,
                         const OCILobLocator *pLocp, ub2 *pCsid);

sword OCILobAppend ( OCISvcCtx *pServiceContexthp,
                                 OCIError *pErrorHandle,
                       OCILobLocator *pDstLocp,
                       OCILobLocator *pSrcLocp);

sword OCILobClose(OCISvcCtx *pSvchp,
                   OCIError *pErrorHandle,
                   OCILobLocator *pLocp);


sword OCILobDisableBuffering (OCISvcCtx *pSvchp,
                                OCIError *pErrorHandle,
                                OCILobLocator *pLocp);

sword OCILobEnableBuffering (OCISvcCtx *pSvchp,
                               OCIError *pErrorHandle,
                               OCILobLocator *pLocp);

sword OCILobErase ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pLocp,
                      ub4 *pAmount, ub4 offset);

sword OCILobFileSetName ( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           OCILobLocator **pFilepp,
                           const OraText *pDirAlias, ub2 dLength,
                           const OraText *pFilename, ub2 fLength);

sword OCILobGetChunkSize(OCISvcCtx *pSvchp,
                         OCIError *pErrorHandle,
                         OCILobLocator *pLocp,
                         ub4 *pChunksizep);

sword OCILobIsOpen(OCISvcCtx *pSvchp,
                    OCIError *pErrorHandle,
                    OCILobLocator *pLocp,
                    int *pFlag);

sword OCILobIsTemporary( OCIEnv *pEnv,
                        OCIError *pErrorHandle,
                        OCILobLocator *pLocp,
                        int *pIsTemporary);

sword OCILobOpen(OCISvcCtx *pSvchp,
                   OCIError *pErrorHandle,
                   OCILobLocator *pLocp,
                   ub1 mode);

sword OCILobRead ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pLocp,
                     ub4 *pAmtp, ub4 offset,
                     void *pBufp, ub4 bufl,
                     void *pCtxp,
                     OCICallbackLobRead cbfp,
                     ub2 csid, ub1 csfrm);

sword OCILobLoadFromFile ( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                            OCILobLocator *pDstLocp,
                            OCILobLocator *pSrcFilep,
                            ub4 amount, ub4 dstOffset,
                            ub4 srcOffset);

sword OCILobLocatorAssign ( OCISvcCtx *pServiceContexthp,
                                        OCIError *pErrorHandle,
                              const OCILobLocator *pSrcLocp,
                              OCILobLocator **pDstLocpp);


sword OCILobLocatorIsInit ( OCIEnv *pEnvironment,
                                       OCIError *pErrorHandle,
                             const OCILobLocator *pLocp,
                             int *pIsInitialized);


sword OCILobGetLength ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCILobLocator *pLocp,
                          ub4 *pLenp);

sword OCILobFlushBuffer (OCISvcCtx *pSvchp,
                           OCIError *pErrorHandle,
                           OCILobLocator *pLocp,
                           ub4 flag);

sword OCILobFreeTemporary(OCISvcCtx *pSvchp,
                          OCIError *pErrorHandle,
                          OCILobLocator *pLocp);

sword OCILobIsEqual ( OCIEnv *pEnvironment,
                        const OCILobLocator *x,
                        const OCILobLocator *y,
                        int *pIsEqual);

sword OCILobTrim ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pLocp,
                     ub4 newLen);

sword OCILobWrite ( OCISvcCtx *pServiceContexthp,
                                OCIError *pErrorHandle, OCILobLocator *pLocp,
                      ub4 *pAmtp, ub4 offset, void *pBufp, ub4 bufLen,
                      ub1 piece, void *pCtxp, OCICallbackLobWrite cbfp,
                      ub2 csid, ub1 csfrm);

sword OCILobGetDeduplicateRegions( OCISvcCtx *pServiceContexthp,
                                            OCIError *pErrorHandle,
                                  OCILobLocator *pLocp,
                                  OCILobRegion *pRegp, ub4 *pCount, ub1 piece,
                                  void *pCtxp,
                                  OCICallbackLobGetDeduplicateRegions cbfp);

sword OCILobWriteAppend( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle, OCILobLocator *pLobp,
                        ub4 *pAmtp, void *pBufp, ub4 bufl, ub1 piece,
                        void *pCtxp, OCICallbackLobWrite cbfp, ub2 csid,
                        ub1 csfrm);

sword OCIBreak (void *pHndlp, OCIError *pErrorHandle);

sword OCIReset (void *pHndlp, OCIError *pErrorHandle);

sword OCIServerVersion (void *pHndlp,
                                     OCIError *pErrorHandle,
                           OraText *pBufp,
                           ub4 bufsz,
                           ub1 hndltype);

sword OCIServerRelease (void *pHndlp,
                                     OCIError *pErrorHandle,
                           OraText *pBufp,
                           ub4 bufsz,
                           ub1 hndltype, ub4 *pVersion);


sword OCILdaToSvcCtx ( OCISvcCtx **pSvchpp,
                                  OCIError *pErrorHandle,
                        Lda_Def *pLdap);

sword OCIResultSetToStmt (OCIResult *pRsetdp,
                            OCIError *pErrorHandle);

sword OCIAttrGet (const void *pTrgthndlp, ub4 trghndltyp,
                    void *pAttributep, ub4 *pSizep, ub4 attrtype,
                    OCIError *pErrorHandle);

sword OCIAttrSet (void *pTrgthndlp, ub4 trghndltyp, void *pAttributep,
                    ub4 size, ub4 attrtype, OCIError *pErrorHandle);

sword OCISvcCtxToLda ( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        Lda_Def *pLdap);

sword OCIUserCallbackRegister(void *pHndlp,
                                ub4 type,
                                void *pEhndlp,
                                OCIUserCallback callback,
                                void *pCtxp,
                                ub4 fcode, ub4 when,
                                OCIUcb *pUcbDesc);

sword OCIUserCallbackGet(void *pHndlp, ub4 type,
                           void *pEhndlp,
                           ub4 fcode, ub4 when,
                           OCIUserCallback *pCallbackp,
                           void **pCtxpp, OCIUcb *pUcbDesc);

sword OCISharedLibInit(void *pMetaCtx, void *pLibCtx, ub4 argfmt, sword argc,
                         void **pArgv, OCIEnvCallbackType envCallback);



sword OCIFileClose ( void *pHndl,
                           OCIError *pErrorHandle,
                          OCIFileObject *pFilep);

sword OCIFileExists ( void *pHndl,
                            OCIError *pErrorHandle,
                           OraText *pFilename,
                            OraText *pPath, ub1 *pFlag );

sword OCIFileGetLength( void *pEnvironment,
                              OCIError *pErrorHandle,
                             OraText *pFilename,
                             OraText *pPath,
                              ubig_ora *pLenp );

sword OCIFileInit ( void *pEnvironment,
                          OCIError *pErrorHandle);

sword OCIFileWrite ( void *pEnvironment,
                           OCIError *pErrorHandle,
                              OCIFileObject *pFilep,
                          void *pBufp, ub4 bufLen,
                           ub4 *pBytesWritten);

sword OCIFileOpen ( void *pEnvironment,
                          OCIError *pErrorHandle,
                          OCIFileObject **pFilep,
                         OraText *pFilename,
                         OraText *pPath, ub4 mode,
                         ub4 create,
                         ub4 type);

sword OCIFileRead ( void *pEnvironment,
                          OCIError *pErrorHandle,
                             OCIFileObject *pFilep,
                         void *pBufp,
                         ub4 bufl,
                          ub4 *pBytesread);

sword OCIFileSeek ( void *pEnvironment,
                          OCIError *pErrorHandle,
                             OCIFileObject *pFilep,
                   uword origin, ubig_ora offset, sb1 dir);

sword OCIFileTerm ( void *pEnvironment,
                          OCIError *pErrorHandle);

sword OCIFileFlush( void *pEnvironment,
                          OCIError *pErrorHandle,
                         OCIFileObject *pFilep );




sword OCILobErase2 ( OCISvcCtx *pServiceContexthp,
                                OCIError *pErrorHandle,
                      OCILobLocator *pLocp,
                      oraub8 *pAmount, oraub8 offset);


sword OCILobGetOptions ( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCILobLocator *pLobp,
                        ub4 optypes, void *pOptionsp,
                        ub4 *pOptionslenp,
                        ub4 mode);

sword OCILobSetOptions ( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCILobLocator *pLobp,
                        ub4 optypes, void *pOptionsp,
                        ub4 optionsLen,
                        ub4 mode);

sword OCILobGetLength2 ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCILobLocator *pLocp,
                          oraub8 *pLenp);

sword OCILobCopy2 ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pDstLocp,
                     OCILobLocator *pSrcLocp, oraub8 amount,
                     oraub8 dstOffset,
                     oraub8 srcOffset);

sword OCILobRead2 ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pLocp,
                     oraub8 *pByteAmtp,
                     oraub8 *pCharAmtp, oraub8 offset,
                     void *pBufp, oraub8 bufl,
                     ub1 piece, void *pCtxp,
                     OCICallbackLobRead2 cbfp,
                     ub2 csid, ub1 csfrm);

sword OCILobLoadFromFile2 ( OCISvcCtx *pServiceContexthp,
                                       OCIError *pErrorHandle,
                             OCILobLocator *pDstLocp,
                             OCILobLocator *pSrcFilep,
                             oraub8 amount, oraub8 dstOffset,
                             oraub8 srcOffset);

sword OCILobWriteAppend2 ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCILobLocator *pLobp,
                          oraub8 *pByteAmtp, oraub8 *pCharAmtp, void *pBufp,
                          oraub8 bufl, ub1 piece, void *pCtxp,
                          OCICallbackLobWrite2 cbfp, ub2 csid, ub1 csfrm);

sword OCILobGetStorageLimit ( OCISvcCtx *pServiceContexthp,
                                       OCIError *pErrorHandle,
                             OCILobLocator *pLobp,
                             oraub8 *pLimitp);

sword OCILobTrim2 ( OCISvcCtx *pServiceContexthp,
                               OCIError *pErrorHandle,
                     OCILobLocator *pLocp,
                     oraub8 newLen);

sword OCILobWrite2 ( OCISvcCtx *pServiceContexthp,
                                OCIError *pErrorHandle, OCILobLocator *pLocp,
                      oraub8 *pByteAmtp, oraub8 *pCharAmtp, oraub8 offset,
                      void *pBufp, oraub8 bufLen, ub1 piece, void *pCtxp,
                      OCICallbackLobWrite2 cbfp, ub2 csid, ub1 csfrm);

sword OCILobArrayWrite ( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle, ub4 *pArrayIter,
                          OCILobLocator **pLobpArr, oraub8 *pByteAmtArr,
                          oraub8 *pCharAmtArr, oraub8 *pOffsetArr,
                          void **pBufpArr, oraub8 *pBuflArr, ub1 piece,
                          void *pCtxp, OCICallbackLobArrayWrite cbfp, ub2 csid,
                          ub1 csfrm);

sword OCILobArrayRead ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle, ub4 *pArrayIter,
                         OCILobLocator **pLobpArr, oraub8 *pByteAmtArr,
                         oraub8 *pCharAmtArr, oraub8 *pOffsetArr,
                         void **pBufpArr, oraub8 *pBuflArr,
                         ub1 piece, void *pCtxp,
                         OCICallbackLobArrayRead cbfp, ub2 csid,
                         ub1 csfrm);





sword OCISecurityStorePersona(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              nzttPersona **pPersona,
                              nzttWallet *pWallet);

sword OCISecurityOpenPersona(OCISecurity *pOsshandle,
                             OCIError *pErrorHandle,
                             nzttPersona *pPersona);

sword OCISecurityClosePersona(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              nzttPersona *pPersona);

sword OCISecurityRemovePersona(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttPersona **pPersona);

sword OCISecurityCreatePersona(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttIdentType identityType,
                               nzttCipherType cipherType,
                               nzttPersonaDesc *pDesc,
                               nzttPersona **pPersona);
sword OCISecurityInitialize (OCISecurity *pSechandle, OCIError *pErrorHandle);

sword OCISecurityTerminate (OCISecurity *pSechandle, OCIError *pErrorHandle);

sword OCISecurityOpenWallet(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            size_t wrlLen,
                            OraText *pWalletResourceLocator,
                            size_t pwdLen,
                            OraText *pPassword,
                            nzttWallet *pWallet);

sword OCISecurityCloseWallet(OCISecurity *pOsshandle,
                             OCIError *pErrorHandle,
                             nzttWallet *pWallet);

sword OCISecurityCreateWallet(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              size_t wrlLen,
                              OraText *pWalletResourceLocator,
                              size_t pwdLen,
                              OraText *pPassword,
                              nzttWallet *pWallet);

sword OCISecurityDestroyWallet(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               size_t wrlLen,
                               OraText *pWalletResourceLocator,
                               size_t pwdLen,
                               OraText *pPassword);


sword OCISecuritySetProtection(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttPersona *pPersona,
                               nzttcef cryptoEngineFunction,
                               nztttdufmt dataUnitFormat,
                               nzttProtInfo *pRotectionInfo);

sword OCISecurityGetProtection(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttPersona *pPersona,
                               nzttcef cryptoEngineFunction,
                               nztttdufmt *pDataUnitFormat,
                               nzttProtInfo *pRotectionInfo);

sword OCISecurityRemoveIdentity(OCISecurity *pOsshandle,
                                OCIError *pErrorHandle,
                                nzttIdentity **pIdentity);

sword OCISecurityCreateIdentity(OCISecurity *pOsshandle,
                                OCIError *pErrorHandle,
                                nzttIdentType type,
                                nzttIdentityDesc *pDesc,
                                nzttIdentity **pIdentity);

sword OCISecurityAbortIdentity(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttIdentity **pIdentity);

sword OCISecurityFreeIdentity(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              nzttIdentity **pIdentity);


sword OCISecurityStoreTrustedIdentity(OCISecurity *pOsshandle,
                                      OCIError *pErrorHandle,
                                      nzttIdentity **pIdentity,
                                      nzttPersona *pPersona);

sword OCISecuritySign(OCISecurity *pOsshandle,
                      OCIError *pErrorHandle,
                      nzttPersona *pPersona,
                      nzttces signatureState,
                      size_t inputLength,
                      ub1 *pInput,
                      nzttBufferBlock *pBufferBlock);

sword OCISecuritySignExpansion(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttPersona *pPersona,
                               size_t inputLen,
                               size_t *pSignatureLength);

sword OCISecurityVerify(OCISecurity *pOsshandle,
                        OCIError *pErrorHandle,
                        nzttPersona *pPersona,
                        nzttces signatureState,
                        size_t sigLen,
                        ub1 *pSignature,
                        nzttBufferBlock *pExtractedMessage,
                        int *pVerified,
                        int *pValidated,
                        nzttIdentity **pSigningPartyIdentity);

sword OCISecurityValidate(OCISecurity *pOsshandle,
                          OCIError *pErrorHandle,
                          nzttPersona *pPersona,
                          nzttIdentity *pIdentity,
                          int *pValidated);

sword OCISecuritySignDetached(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              nzttPersona *pPersona,
                              nzttces signatureState,
                              size_t inputLength,
                              ub1 *pInput,
                              nzttBufferBlock *pSignature);

sword OCISecuritySignDetExpansion(OCISecurity *pOsshandle,
                                  OCIError *pErrorHandle,
                                  nzttPersona *pPersona,
                                  size_t inputLength,
                                  size_t *pRequiredbUfferLength);

sword OCISecurityVerifyDetached(OCISecurity *pOsshandle,
                                OCIError *pErrorHandle,
                                nzttPersona *pPersona,
                                nzttces signatureState,
                                size_t dataLength,
                                ub1 *pData,
                                size_t sigLen,
                                ub1 *pSignature,
                                int *pVerified,
                                int *pValidated,
                                nzttIdentity **pSigningPartyIdentity);

sword OCISecurity_PKEncrypt(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            nzttPersona *pPersona,
                            size_t numberOfRecipients,
                            nzttIdentity *pRecipientList,
                            nzttces encryptionState,
                            size_t inputLength,
                            ub1 *pInput,
                            nzttBufferBlock *pEncryptedData);

sword OCISecurityPKEncryptExpansion(OCISecurity *pOsshandle,
                                    OCIError *pErrorHandle,
                                    nzttPersona *pPersona,
                                    size_t numberRecipients,
                                    size_t inputLength,
                                    size_t *pBufferLengthRequired);

sword OCISecurityPKDecrypt(OCISecurity *pOsshandle,
                           OCIError *pErrorHandle,
                           nzttPersona *pPersona,
                           nzttces encryptionState,
                           size_t inputLength,
                           ub1 *pInput,
                           nzttBufferBlock *pEncryptedData);

sword OCISecurityEncrypt(OCISecurity *pOsshandle,
                         OCIError *pErrorHandle,
                         nzttPersona *pPersona,
                         nzttces encryptionState,
                         size_t inputLength,
                         ub1 *pInput,
                         nzttBufferBlock *pEncryptedData);

sword OCISecurityEncryptExpansion(OCISecurity *pOsshandle,
                                  OCIError *pErrorHandle,
                                  nzttPersona *pPersona,
                                  size_t inputLength,
                                  size_t *pEncryptedDataLength);

sword OCISecurityDecrypt(OCISecurity *pOsshandle,
                         OCIError *pErrorHandle,
                         nzttPersona *pPersona,
                         nzttces decryptionState,
                         size_t inputLength,
                         ub1 *pInput,
                         nzttBufferBlock *pDecryptedData);

sword OCISecurityEnvelope(OCISecurity *pOsshandle,
                          OCIError *pErrorHandle,
                          nzttPersona *pPersona,
                          size_t numberOfRecipients,
                          nzttIdentity *pIdentity,
                          nzttces encryptionState,
                          size_t inputLength,
                          ub1 *pInput,
                          nzttBufferBlock *pEnvelopedData);

sword OCISecurityDeEnvelope(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            nzttPersona *pPersona,
                            nzttces decryptionState,
                            size_t inputLength,
                            ub1 *pInput,
                            nzttBufferBlock *pOutputMessage,
                            int *pVerified,
                            int *pValidated,
                            nzttIdentity **pSenderIdentity);

sword OCISecurityKeyedHash(OCISecurity *pOsshandle,
                           OCIError *pErrorHandle,
                           nzttPersona *pPersona,
                           nzttces hashState,
                           size_t inputLength,
                           ub1 *pInput,
                           nzttBufferBlock *pKeyedHash);

sword OCISecurityKeyedHashExpansion(OCISecurity *pOsshandle,
                                    OCIError *pErrorHandle,
                                    nzttPersona *pPersona,
                                    size_t inputLength,
                                    size_t *pRequiredbUfferLength);

sword OCISecurityHash(OCISecurity *pOsshandle,
                      OCIError *pErrorHandle,
                      nzttPersona *pPersona,
                      nzttces hashState,
                      size_t input,
                      ub1 *pInputLength,
                      nzttBufferBlock *pHash);

sword OCISecurityHashExpansion(OCISecurity *pOsshandle,
                               OCIError *pErrorHandle,
                               nzttPersona *pPersona,
                               size_t inputLength,
                               size_t *pRequiredbUfferLength);

sword OCISecuritySeedRandom(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            nzttPersona *pPersona,
                            size_t seedLength,
                            ub1 *pSeed);

sword OCISecurityRandomBytes(OCISecurity *pOsshandle,
                             OCIError *pErrorHandle,
                             nzttPersona *pPersona,
                             size_t numberOfBytesDesired,
                             nzttBufferBlock *pRandomBytes);

sword OCISecurityRandomNumber(OCISecurity *pOsshandle,
                              OCIError *pErrorHandle,
                              nzttPersona *pPersona,
                              uword *pRandomNumber);

sword OCISecurityInitBlock(OCISecurity *pOsshandle,
                           OCIError *pErrorHandle,
                           nzttBufferBlock *pBufferBlock);

sword OCISecurityReuseBlock(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            nzttBufferBlock *pBufferBlock);

sword OCISecurityPurgeBlock(OCISecurity *pOsshandle,
                            OCIError *pErrorHandle,
                            nzttBufferBlock *pBufferBlock);

sword OCISecuritySetBlock(OCISecurity *pOsshandle,
                          OCIError *pErrorHandle,
                          uword flagsToSet,
                          size_t bufferLength,
                          size_t usedbUfferLength,
                          ub1 *pBuffer,
                          nzttBufferBlock *pBufferBlock);

sword OCISecurityGetIdentity(OCISecurity *pOsshandle,
                             OCIError *pErrorHandle,
                             size_t nameLen,
                             OraText *pDistinguishedName,
                             nzttIdentity **pIdentity);

sword OCIExtractInit( void *pEnvironment,
                               OCIError *pErrorHandle);

sword OCIAQDeqArray( OCISvcCtx *pServiceContexthp,
                              OCIError *pErrorHandle,
                    OraText *pQueueName,
                    OCIAQDeqOptions *pDeqopt,
                    ub4 *pIters,
                    OCIAQMsgProperties **pMsgprop,
                    OCIType *pPayloadTdo,
                    void **pPayload,
                    void **pPayloadInd,
                    OCIRaw **pMsgid,
                    void *pCtxp,
                    OCICallbackAQDeq deqcbfp,
                    ub4 flags);

sword OCIAQDeq( OCISvcCtx *pServiceContexthp,
                         OCIError *pErrorHandle,
               OraText *pQueueName,
               OCIAQDeqOptions *pDeqopt,
               OCIAQMsgProperties *pMsgprop,
               OCIType *pPayloadTdo,
               void **pPayload,
               void **pPayloadInd,
               OCIRaw **pMsgid, ub4 flags);

sword OCIAQEnqArray( OCISvcCtx *pServiceContexthp,
                              OCIError *pErrorHandle,
                    OraText *pQueueName,
                    OCIAQEnqOptions *pEnqopt,
                    ub4 *pIters,
                    OCIAQMsgProperties **pMsgprop,
                    OCIType *pPayloadTdo,
                    void **pPayload,
                    void **pPayloadInd,
                    OCIRaw **pMsgid,
                    void *pCtxp, OCICallbackAQEnq enqcbfp,
                    ub4 flags);

sword OCIAQEnq( OCISvcCtx *pServiceContexthp,
                         OCIError *pErrorHandle,
               OraText *pQueueName,
               OCIAQEnqOptions *pEnqopt,
               OCIAQMsgProperties *pMsgprop,
               OCIType *pPayloadTdo,
               void **pPayload,
               void **pPayloadInd,
               OCIRaw **pMsgid, ub4 flags);

sword OCIAQEnqStreaming( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OraText *pQueueName,
                        OCIAQEnqOptions *pEnqopt,
                        OCIType *pPayloadTdo,
                        void *pCtxp,
                        OCICallbackAQEnqStreaming enqcbfp,
                        ub4 flags);

sword OCIAQListen2( OCISvcCtx *pServiceContexthp,
                             OCIError *pErrorHandle,
                   OCIAQAgent **pAgentList,
                   ub4 numAgents,
                   OCIAQListenOpts *pLopts,
                   OCIAQAgent **pAgent,
                   OCIAQLisMsgProps *pLmops,
                   ub4 flags);

sword OCIAQGetReplayInfo( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                         OraText *pQueueName,
                         OCIAQAgent *pSender,
                         ub4 replayAttribute,
                         OraText *pCorrelation,
                         ub2 *pCorrLen);

sword OCIAQListen( OCISvcCtx *pServiceContexthp,
                            OCIError *pErrorHandle,
                  OCIAQAgent **pAgentList,
                  ub4 numAgents,
                  sb4 wait,
                  OCIAQAgent **pAgent,
                  ub4 flags);

sword OCIAQResetReplayInfo( OCISvcCtx *pServiceContexthp,
                                     OCIError *pErrorHandle,
                           OraText *pQueueName,
                           OCIAQAgent *pSender,
                           ub4 replayAttribute);


sword OCIExtractReset( void *pEnvironment,
                                OCIError *pErrorHandle);

sword OCIExtractSetNumKeys( void *pEnvironment,
                                     OCIError *pErrorHandle,
                                 uword numkeys);

sword OCIExtractFromStr( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              ub4 flag,
                              OraText *pInput);

sword OCIExtractToInt( void *pEnvironment,
                                OCIError *pErrorHandle, OraText *pKeyName,
                            uword valno,
                             sb4 *pRetval);

sword OCIExtractSetKey( void *pEnvironment,
                                 OCIError *pErrorHandle,
                             const OraText *pName,
                             ub1 type, ub4 flag,
                             const void *pDefval,
                             const sb4 *pIntrange,
                             const OraText *const *pStrlist);

sword OCIExtractFromFile( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               ub4 flag,
                               OraText *pFilename);

sword OCIExtractFromList( void *pEnvironment,
                                   OCIError *pErrorHandle,
                                uword index,
                                OraText **pName,
                                ub1 *pType,
                                uword *pNumvals,
                                void ***pValues);

sword OCIExtractToStr( void *pEnvironment,
                                OCIError *pErrorHandle,
                            OraText *pKeyName,
                            uword valno,
                             OraText *pRetval,
                            uword bufLen);

sword OCIExtractToBool( void *pEnvironment,
                                 OCIError *pErrorHandle,
                             OraText *pKeyName,
                             uword valno,
                              ub1 *pRetval);

sword OCIExtractToOCINum( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               OraText *pKeyName,
                               uword valno,
                                OCINumber *pRetval);

sword OCIExtractToList( void *pEnvironment,
                                 OCIError *pErrorHandle,
                              uword *pNumkeys);

sword OCIExtractTerm( void *pEnvironment,
                               OCIError *pErrorHandle);


sword OCIMemoryResize(void *pHdl,
                                OCIError *pErrorHandle,
                      void **pMem,
                      ub4 newSize,
                      ub4 flags);

sword OCIMemoryAlloc(void *pHdl,
                               OCIError *pErrorHandle,
                     void **pMem,
                     OCIDuration dur, ub4 size,
                     ub4 flags);

sword OCIMemoryFree(void *pHdl, OCIError *pErrorHandle,
                    void *pMem);


sword OCIContextClearValue(void *pHdl,
                                     OCIError *pErrorHandle,
                           ub1 *pKey,
                           ub1 keyLen);

sword OCIContextGenerateKey(void *pHdl,
                                      OCIError *pErrorHandle,
                            ub4 *pKey);

sword OCIContextSetValue(void *pHdl, OCIError *pErrorHandle,
                         OCIDuration duration,
                         ub1 *pKey, ub1 keyLen,
                         void *pCtxValue);

sword OCIContextGetValue(void *pHdl,
                                   OCIError *pErrorHandle,
                         ub1 *pKey,
                         ub1 keyLen,
                         void **pCtxValue);

sword OCIMemorySetCurrentIDs(void *pHdl,
                                       OCIError *pErrorHandle,
                             ub4 currSessionId,
                             ub4 currTransId,
                             ub4 currStmtId);


sword OCIPicklerTdsCtxInit( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           OCIPicklerTdsCtx **pTdsc);

sword OCIPicklerTdsInit( OCIEnv *pEnvironment,
                                  OCIError *pErrorHandle,
                        OCIPicklerTdsCtx *pTdsc,
                        OCIPicklerTds **pTdsh);

sword OCIPicklerTdsCreateElementChar( OCIEnv *pEnvironment,
                                               OCIError *pErrorHandle,
                                     OCIPicklerTds *pTdsh, ub2 Len,
                                     OCIPicklerTdsElement *pElt);

sword OCIPicklerTdsCreateElementVarchar( OCIEnv *pEnvironment,
                                                  OCIError *pErrorHandle,
                                        OCIPicklerTds *pTdsh, ub2 Len,
                                        OCIPicklerTdsElement *pElt);

sword OCIPicklerTdsFree( OCIEnv *pEnvironment,
                                  OCIError *pErrorHandle,
                        OCIPicklerTds *pTdsh);

sword OCIPicklerTdsCreateElementNumber( OCIEnv *pEnvironment,
                                                 OCIError *pErrorHandle,
                                       OCIPicklerTds *pTdsh, ub1 prec,
                                       sb1 scale, OCIPicklerTdsElement *pElt);

sword OCIPicklerTdsCtxFree( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           OCIPicklerTdsCtx *pTdsc);

sword OCIPicklerImageCollAddScalar( OCIEnv *pEnvironment,
                                             OCIError *pErrorHandle,
                                   OCIPicklerImage *pImgh, void *pScalar,
                                   ub4 bufLen, OCIInd ind);

sword OCIPicklerImageCollEnd( OCIEnv *pEnvironment,
                                       OCIError *pErrorHandle,
                             OCIPicklerImage *pImgh);

sword OCIPicklerTdsCreateElementRaw( OCIEnv *pEnvironment,
                                              OCIError *pErrorHandle,
                                    OCIPicklerTds *pTdsh,
                                    ub2 Len,
                                    OCIPicklerTdsElement *pElt);

sword OCIPicklerTdsCreateElement( OCIEnv *pEnvironment,
                                           OCIError *pErrorHandle,
                                 OCIPicklerTds *pTdsh, OCITypeCode dty,
                                 OCIPicklerTdsElement *pElt);

sword OCIPicklerTdsAddAttr( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           OCIPicklerTds *pTdsh, OCIPicklerTdsElement elt);

sword OCIPicklerTdsGenerate( OCIEnv *pEnvironment,
                                      OCIError *pErrorHandle,
                            OCIPicklerTds *pTdsh);

sword OCIPicklerTdsGetAttr( OCIEnv *pEnvironment,
                                     OCIError *pErrorHandle,
                           const OCIPicklerTds *pTdsh, ub1 attrno,
                           OCITypeCode *pTyp, ub2 *pLen);

sword OCIPicklerFdoInit( OCIEnv *pEnvironment,
                                  OCIError *pErrorHandle,
                                OCIPicklerFdo **pFdoh);

sword OCIPicklerFdoFree( OCIEnv *pEnvironment,
                                  OCIError *pErrorHandle,
                        OCIPicklerFdo *pFdoh);

sword OCIPicklerImageInit( OCIEnv *pEnvironment,
                                    OCIError *pErrorHandle,
                          OCIPicklerFdo *pFdoh,
                          OCIPicklerTds *pTdsh,
                          OCIPicklerImage **pImgh);

sword OCIPicklerImageFree( OCIEnv *pEnvironment,
                                    OCIError *pErrorHandle,
                                OCIPicklerImage *pImgh);

sword OCIPicklerImageAddScalar( OCIEnv *pEnvironment,
                                         OCIError *pErrorHandle,
                               OCIPicklerImage *pImgh,
                               void *pScalar, ub4 len);

sword OCIPicklerImageGenerate( OCIEnv *pEnvironment,
                                        OCIError *pErrorHandle,
                                OCIPicklerImage *pImgh);

sword OCIPicklerImageGetScalarSize( OCIEnv *pEnvironment,
                                             OCIError *pErrorHandle,
                                OCIPicklerImage *pImgh,
                                ub4 attrno, ub4 *pSize);

sword OCIPicklerImageGetScalar( OCIEnv *pEnvironment,
                                         OCIError *pErrorHandle,
                                OCIPicklerImage *pImgh, ub4 attrno,
                                void *pBuffer, ub4 *pLen, OCIInd *pInd);

sword OCIPicklerImageAddNullScalar( OCIEnv *pEnvironment,
                                             OCIError *pErrorHandle,
                                OCIPicklerImage *pImgh);

sword OCIPicklerImageCollBegin( OCIEnv *pEnvironment,
                                         OCIError *pErrorHandle,
                               OCIPicklerImage *pImgh,
                               const OCIPicklerTds *pColltdsh);

sword OCIPicklerImageCollBeginScan( OCIEnv *pEnvironment,
                                             OCIError *pErrorHandle,
                                   OCIPicklerImage *pImgh,
                                   const OCIPicklerTds *pCollTdsh,
                                   ub4 attrnum, ub4 startidx,
                                   OCIInd *pInd);

sword OCIPicklerImageCollGetScalarSize( OCIEnv *pEnvironment,
                                                 OCIError *pErrorHandle,
                                       const OCIPicklerTds *pCollTdsh,
                                       ub4 *pSize);

sword OCIPicklerImageCollGetScalar( OCIEnv *pEnvironment,
                                             OCIError *pErrorHandle,
                                   OCIPicklerImage *pImgh, void *pBuffer,
                                   ub4 *pBufferLen, OCIInd *pInd);


sword OCIAnyDataDestroy( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCIAnyData *pSdata);

sword OCIAnyDataCollAddElem( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                            OCIAnyData *pSdata,
                            OCITypeCode typeCode,
                            OCIType *pType,
                            void *pInd,
                            void *pAttrVal,
                            ub4 length,
                            int isAny,
                            int lastElem);

sword OCIAnyDataAttrSet( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCIAnyData *pSdata,
                        OCITypeCode typeCode,
                        OCIType *pType,
                        void *pInd, void *pAttrVal,
                        ub4 length, int isAny);

sword OCIAnyDataIsNull( OCISvcCtx *pServiceContexthp,
                                 OCIError *pErrorHandle,
                       OCIAnyData *pSdata,
                       int *pIsnull);

sword OCIAnyDataConvert( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCITypeCode typeCode,
                        OCIType *pType, OCIDuration dur,
                        void *pInd, void *pDataVal,
                        ub4 Len, OCIAnyData **pSdata);

sword OCIAnyDataBeginCreate( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                            OCITypeCode typeCode,
                            OCIType *pType, OCIDuration dur,
                            OCIAnyData **pSdata);

sword OCIAnyDataGetType( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCIAnyData *pSdata,
                        OCITypeCode *pTc,
                        OCIType **pType);

sword OCIAnyDataEndCreate( OCISvcCtx *pServiceContexthp,
                                    OCIError *pErrorHandle,
                          OCIAnyData *pSdata);

sword OCIAnyDataAccess( OCISvcCtx *pServiceContexthp,
                                 OCIError *pErrorHandle,
                       OCIAnyData *pSdata,
                       OCITypeCode typeCode,
                       OCIType *pType,
                       void *pInd,
                       void *pAttrVal,
                       ub4 *pLength);

sword OCIAnyDataCollGetElem( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                            OCIAnyData *pSdata,
                            OCITypeCode typeCode,
                            OCIType *pType,
                            void *pInd,
                            void *pCelemVal,
                            ub4 *pLength,
                            int isAny);

sword OCIAnyDataSetBeginCreate( OCISvcCtx *pServiceContexthp,
                                         OCIError *pErrorHandle,
                                     OCITypeCode typeCode,
                                     const OCIType *pType,
                                     OCIDuration duration,
                                      OCIAnyDataSet ** pDataSet);


sword OCIAnyDataSetDestroy( OCISvcCtx *pServiceContexthp,
                                     OCIError *pErrorHandle,
                                      OCIAnyDataSet *pDataSet);

sword OCIAnyDataSetGetType ( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                                  OCIAnyDataSet *pDataSet,
                                   OCITypeCode *pTc,
                                   OCIType **pType);

sword OCIAnyDataSetGetCount( OCISvcCtx *pServiceContexthp,
                                      OCIError *pErrorHandle,
                                  OCIAnyDataSet *pDataSet,
                                   ub4 *pCount);

sword OCIAnyDataGetCurrAttrNum( OCISvcCtx *pServiceContexthp,
                                         OCIError *pErrorHandle,
                               OCIAnyData *pSdata, ub4 *pAttrnum);

sword OCIAnyDataAttrGet( OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle,
                        OCIAnyData *pSdata,
                        OCITypeCode typeCode,
                        OCIType *pType,
                        void *pInd,
                        void *pAttrVal,
                        ub4 *pLength,
                        int isAny);

sword OCIAnyDataSetGetInstance( OCISvcCtx *pServiceContexthp,
                                         OCIError *pErrorHandle,
                                     OCIAnyDataSet *pDataSet,
                                         OCIAnyData **pData);

sword OCIAnyDataSetAddInstance( OCISvcCtx *pServiceContexthp,
                                         OCIError *pErrorHandle,
                                         OCIAnyDataSet *pDataSet,
                                         OCIAnyData **pData);

sword OCIAnyDataSetEndCreate( OCISvcCtx *pServiceContexthp,
                                       OCIError *pErrorHandle,
                                       OCIAnyDataSet *pDataSet);


sword OCIFormatTerm( void *pEnvironment,
                    OCIError *pErrorHandle);

sword OCIFormatInit( void *pEnvironment, OCIError *pErrorHandle);

sword OCIFormatString( void *pEnvironment,
                                OCIError *pErrorHandle,
                      OraText *pBuffer,
                      sbig_ora bufferLength,
                      sbig_ora *pReturnLength,
                      const OraText *pFormatString, ...);

sword OCIFormatTEb2(void);
sword OCIFormatTSb1(void);
sword OCIFormatTUb2(void);
sword OCIFormatTSb4(void);
sword OCIFormatTUbig_ora(void);
sword OCIFormatTText(void);
sword OCIFormatTSword(void);
sword OCIFormatTSbig_ora(void);
sword OCIFormatTEword(void);
sword OCIFormatTEb1(void);
sword OCIFormatTEb4(void);
sword OCIFormatTDouble(void);
sword OCIFormatTDvoid(void);
sword OCIFormatTEnd(void);
sword OCIFormatTChar(void);
sword OCIFormatTUb1(void);
sword OCIFormatTSb2(void);
sword OCIFormatTUb4(void);
sword OCIFormatTUword(void);

sword OCINlsGetInfo( void *pEnvironment,
                              OCIError *pErrorHandle,
                           OraText *pBuffer,
                          size_t bufferLen,
                          ub2 item);

OCISvcCtx *xaoSvcCtx( OraText *pDbName);

OCISvcCtx *xaosvch( OraText *pDbName);

int xaosterr( OCISvcCtx *pServiceContexth,
             sb4 errCode);

OCIEnv *xaoEnv( OraText *pDbName);


OCIWchar *OCIWideCharStrrchr( void *pEnvironment,
                                   const OCIWchar *pSourceStr,
                                   OCIWchar wChar);

size_t OCIWideCharStrCaseConversion( void *pEnvironment,
                                           OCIWchar *pDestStr,
                                          const OCIWchar *pSourceStr,
                                          ub4 convertFlag);

size_t OCIWideCharDisplayLength( void *pEnvironment,
                                      OCIWchar wChar);

ub2 OCINlsCharSetNameToId( void *pEnvironment,
                                const oratext *pName);

sword OCINlsNameMap( void *pEnvironment,
                           oratext *pBuffer,
                          size_t bufferLen,
                          const oratext *pSrcbuf,
                          ub4 flag);

sword OCINlsNumericInfoGet( void *pEnvironment,
                                     OCIError *pErrorHandle,
                                  sb4 *pOutputNumVariable,
                                 ub2 environmentItem);

sword OCINlsCharSetIdToName( void *pEnvironment,
                                   oratext *pBuffer,
                                  size_t bufferLen,
                                  ub2 setId);


sword OCIMultiByteInSizeToWideChar( void *pEnvironment,
                                          OCIWchar *pDestination,
                                         size_t destBufferSize,
                                         const OraText *pSource,
                                         size_t sourceStringLen,
                                          size_t *pStrOutputSize);

sword OCIMultiByteToWideChar( void *pEnvironment,
                                    OCIWchar *pDestination,
                                   const OraText *pSourceStr,
                                    size_t *pStrOutputSize);



sword OCIWideCharInSizeToMultiByte( void *pEnvironment,
                                          OraText *pDestination,
                                           size_t destinationBufferSize,
                                          const OCIWchar *pSource,
                                          size_t sourceBufferSize,
                                           size_t *pStrOutputSize);

int OCIWideCharIsAlnum( void *pEnvironment,
                                 OCIWchar wChar);

sword OCIWideCharToMultiByte( void *pEnvironment,
                                    OraText *pDestination,
                                   const OCIWchar *pSource,
                                    size_t *pStrOutputSize);

int OCIWideCharIsLower( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsPrint( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharStrcmp( void *pEnvironment,
                            const OCIWchar *pWCharStr1,
                            const OCIWchar *pWCharStr2,
                            int flag);

int OCIWideCharStrncmp( void *pEnvironment,
                             const OCIWchar *pWCharStr1,
                             size_t strLen1,
                             const OCIWchar *pWCharStr2,
                             size_t strLen2,
                             int flag);

int OCIWideCharIsCntrl( void *pEnvironment,
                                 OCIWchar wChar);

size_t OCIWideCharStrcat( void *pEnvironment,
                                   OCIWchar *pDestWCharStr,
                               const OCIWchar *pSourceWCharStr);

OCIWchar *OCIWideCharStrchr( void *pEnvironment,
                                  const OCIWchar *pWCharStr,
                                  OCIWchar searchWChar);

int OCIWideCharIsDigit( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsGraph( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsSpace( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsUpper( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsXdigit( void *pEnvironment,
                                  OCIWchar wChar);

int OCIWideCharIsSingleByte( void *pEnvironment,
                                      OCIWchar wChar);

OCIWchar OCIWideCharToLower( void *pEnvironment,
                                  OCIWchar wChar);

OCIWchar OCIWideCharToUpper( void *pEnvironment,
                                  OCIWchar wChar);


size_t OCIWideCharStrcpy( void *pEnvironment,
                                OCIWchar *pDestWCharStr,
                               const OCIWchar *pSourceWCharStr);

size_t OCIWideCharStrlen( void *pEnvironment,
                               const OCIWchar *pSourceWCharStr);

size_t OCIWideCharStrncat( void *pEnvironment,
                                   OCIWchar *pDestWCharStr,
                               const OCIWchar *pSourceWCharStr,
                               size_t strAppendLen);

size_t OCIWideCharStrncpy( void *pEnvironment,
                                   OCIWchar *pDestWCharStr,
                               const OCIWchar *pSourceWCharStr,
                               size_t strCopyLen);

size_t OCIWideCharMultiByteLength( void *pEnvironment,
                                        OCIWchar wChar);

int OCIWideCharIsPunct( void *pEnvironment,
                                 OCIWchar wChar);

int OCIWideCharIsAlpha( void *pEnvironment,
                                 OCIWchar wChar);


sword SQLEnvGet(void *ctx, OCIEnv **pEnvironment);
sword SQLSvcCtxGet(void *ctx, text *dbname, sb4 dbnamelen, OCISvcCtx **pSvcCtx);



int OCIMultiByteStrcmp( void *pEnvironment,
                             const OraText *pStr1,
                             const OraText *pStr2,
                             int comparisonMethodFlag);

int OCIMultiByteStrncmp( void *pEnvironment,
                              const OraText *pStr1,
                              size_t pStrLen1,
                              OraText *pStr2,
                              size_t pStrLen2,
                              int comparisonMethodFlag);

size_t OCIMultiByteStrcat( void *pEnvironment,
                                    OraText *pDestStr,
                                const OraText *pSourceStr);

size_t OCIMultiByteStrcpy( void *pEnvironment,
                                 OraText *pDestStr,
                                const OraText *pSourceStr);

size_t OCIMultiByteStrlen( void *pEnvironment,
                                const OraText *pStr);

size_t OCIMultiByteStrncat( void *pEnvironment,
                                  OraText *pDestStr,
                                 const OraText *pSourceStr,
                                 size_t strAppendLen);

size_t OCIMultiByteStrncpy( void *pEnvironment,
                                  OraText *pDestStr,
                                 const OraText *pSourceStr,
                                 size_t strCopyLen);

size_t OCIMultiByteStrnDisplayLength( void *pEnvironment,
                                           const OraText *pStr1,
                                           size_t num);

size_t OCIMultiByteStrCaseConversion( void *pEnvironment,
                                           OraText *pDestStr,
                                          const OraText *pSourceStr,
                                          ub4 convertFlag);


int OCICharSetConversionIsReplacementUsed( void *pEnvironment);

sword OCICharSetToUnicode( void *pEnvironment,
                                 ub2 *pDestBuffer,
                                size_t destBufferLen,
                                const OraText *pSourceStr,
                                size_t sourceLen,
                                 size_t *pStrOutputSize);

sword OCINlsEnvironmentVariableGet( void *pBuffer,
                                         size_t sizeBuffer,
                                         ub2 itemValue,
                                         ub2 CharacterSet,
                                          size_t *pStrOutputSize);

OraText *OCIMessageGet( OCIMsg *pMsgh,
                             ub4 messageNum,
                              OraText *pMessageBuffer,
                             size_t bufLen);

sword OCIMessageClose( void *pEnvironment,
                                OCIError *pErrorHandle,
                                OCIMsg *pMessage);

sword OCIMessageOpen( void *pEnvironment,
                               OCIError *pErrorHandle,
                            OCIMsg **pMassage,
                           const OraText *pRoductName,
                           const OraText *pFacilityName,
                           OCIDuration duration);


sword OCIUnicodeToCharSet( void *pEnvironment,
                                 OraText *pDestination,
                                size_t destBufferLen,
                                const ub2 *pSourceStr,
                                size_t sourceLen,
                                 size_t *pStrOutputSize);

sword OCINlsCharSetConvert( void *pEnvironment,
                                     OCIError *pErrorHandle,
                                 ub2 destId,
                                  void *pDestBuffer,
                                 size_t destLen,
                                 ub2 sourceId,
                                 const void *pSrcp,
                                 size_t sourceBuffer,
                                  size_t *pStrOutputSize);







int OCIThreadIsMulti();

sword OCIThreadMutexInit( void *pEnvironment,
                                   OCIError *pErrorHandle,
                                   OCIThreadMutex **pMutex);

sword OCIThreadMutexDestroy( void *pEnvironment,
                                      OCIError *pErrorHandle,
                                      OCIThreadMutex **pMutex);

sword OCIThreadIdSetNull( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               OCIThreadId *pThreadId);

sword OCIThreadIdGet( void *pEnvironment,
                               OCIError *pErrorHandle,
                           OCIThreadId *pThreadId);

sword OCIThreadMutexAcquire( void *pEnvironment,
                                      OCIError *pErrorHandle,
                                      OCIThreadMutex *pMutex);

void OCIThreadProcessInit();

sword OCIThreadInit( void *pEnvironment,
                              OCIError *pErrorHandle);

sword OCIThreadTerm( void *pEnvironment,
                              OCIError *pErrorHandle);

sword OCIThreadJoin( void *pEnvironment,
                              OCIError *pErrorHandle,
                          OCIThreadHandle *pThread);

sword OCIThreadHandleGet( void *pEnvironment,
                                   OCIError *pErrorHandle,
                                   OCIThreadHandle *pThread);

sword OCIThreadKeyGet( void *pEnvironment,
                                OCIError *pErrorHandle,
                            OCIThreadKey *pKey,
                      void **pValue);

sword OCIThreadIdDestroy( void *pEnvironment,
                                   OCIError *pErrorHandle,
                                   OCIThreadId **pTid);

sword OCIThreadIdSet( void *pEnvironment,
                               OCIError *pErrorHandle,
                            OCIThreadId *pThreadIdDest,
                            OCIThreadId *pThreadIdSrc);

sword OCIThreadIdNull( void *pEnvironment,
                                OCIError *pErrorHandle,
                            OCIThreadId *pThreadId,
                                int *pResult);

sword OCIThreadKeySet( void *pEnvironment,
                                OCIError *pErrorHandle,
                            OCIThreadKey *pKey,
                            void *pThreadValue);

sword OCIThreadIdInit( void *pEnvironment,
                                OCIError *pErrorHandle,
                                OCIThreadId **pTid);

sword OCIThreadHndInit( void *pEnvironment,
                                 OCIError *pErrorHandle,
                       OCIThreadHandle **pThnd);

sword OCIThreadHndDestroy( void *pEnvironment,
                                    OCIError *pErrorHandle,
                          OCIThreadHandle **pThnd);

sword OCIThreadClose( void *pEnvironment,
                               OCIError *pErrorHandle,
                               OCIThreadHandle *pThread);

sword OCIThreadMutexRelease( void *pEnvironment,
                                      OCIError *pErrorHandle,
                                      OCIThreadMutex *pMutex);

sword OCIThreadKeyInit( void *pEnvironment,
                                 OCIError *pErrorHandle,
                              OCIThreadKey **pNewKey,
                             OCIThreadKeyDestFunc destructorForKey);

sword OCIThreadKeyDestroy( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                    OCIThreadKey **pKey);

sword OCIThreadCreate( void *pEnvironment,
                                OCIError *pErrorHandle,
                            void (*pStart)(void *),
                            void *pArgument,
                            OCIThreadId *pThreadId,
                                OCIThreadHandle *pThread);

sword OCIThreadIdSame( void *pEnvironment,
                                OCIError *pErrorHandle,
                            OCIThreadId *pThreadId1,
                            OCIThreadId *pThreadId2,
                                int *pResult);






sword OCISubscriptionUnRegister( OCISvcCtx *pServiceContexthp,
                                OCISubscription *pSubscrhp,
                                          OCIError *pErrorHandle,
                                ub4 mode);

sword OCISubscriptionPost( OCISvcCtx *pServiceContexthp,
                          OCISubscription **pSubscrhpp,
                          ub2 count,
                                    OCIError *pErrorHandle,
                          ub4 mode);

sword OCISubscriptionEnable(OCISubscription *pSubscrhp,
                                      OCIError *pErrorHandle,
                            ub4 mode);

sword OCISubscriptionDisable(OCISubscription *pSubscrhp,
                                       OCIError *pErrorHandle,
                             ub4 mode);

sword OCISubscriptionRegister( OCISvcCtx *pServiceContexthp,
                              OCISubscription **pSubscrhpp,
                              ub2 count,
                                        OCIError *pErrorHandle,
                              ub4 mode);



sword OCIDateTimeConstruct( void *pHndl,
                                     OCIError *pErrorHandle,
                                 OCIDateTime *pDateTime,
                                 sb2 year,
                                 ub1 month,
                                 ub1 day,
                                 ub1 hour,
                                 ub1 min,
                                 ub1 sec,
                                 ub4 fractionalSeconds,
                                 OraText *pTimeZone,
                                 size_t timezoneLen);


sword OCIDateTimeSysTimeStamp( void *pEnvironment,
                                        OCIError *pErrorHandle,
                                     OCIDateTime *pSysDate);

sword OCIDateTimeSubtract( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                OCIDateTime *pIndate1,
                                OCIDateTime *pIndate2,
                                 OCIInterval *pInterval);

sword OCIDateTimeIntervalAdd( void *pEnvironment,
                                       OCIError *pErrorHandle,
                                   OCIDateTime *pDateTime,
                                   OCIInterval *pInterval,
                                    OCIDateTime *pResultDateTime);

sword OCIDateTimeToText( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              const OCIDateTime *pDate,
                              const oratext *pConvFormat,
                              ub1 convFormatLen,
                              ub1 fractionlSeconds,
                              const oratext *pLanguageName,
                              size_t langLength,
                              ub4 *pBufferSize,
                               oratext *pBuffer);

sword OCIDateTimeFromText( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                const OraText *pDdateStr,
                                size_t dstrLength,
                                const oratext *pConvFormat,
                                ub1 convFormatLen,
                                const oratext *pLanguageName,
                                size_t langLength,
                                OCIDateTime *pDate);

sword OCIDateTimeIntervalSub( void *pEnvironment,
                                       OCIError *pErrorHandle,
                                   OCIDateTime *pDateTime,
                                   OCIInterval *pInterval,
                                    OCIDateTime *pResultDateTime);

sword OCIDateTimeAssign( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              const OCIDateTime *pSourceDateTime,
                               OCIDateTime *pTargetDateTime);

sword OCIDateTimeCompare( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               const OCIDateTime *pDdate1,
                               const OCIDateTime *pDate2,
                                sword *pResult);

sword OCIDateTimeGetDate( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               const OCIDateTime *pDate,
                                sb2 *pYear,
                                ub1 *pMonth,
                                ub1 *pDy);

sword OCIDateTimeGetTimeZoneOffset( void *pEnvironment,
                                             OCIError *pErrorHandle,
                                         const OCIDateTime *pDateTime,
                                          sb1 *pHour,
                                          sb1 *pMm);

sword OCIDateTimeCheck( void *pEnvironment,
                                 OCIError *pErrorHandle,
                             const OCIDateTime *pCheckedDate,
                              ub4 *pValid);

sword OCIDateTimeGetTimeZoneName( void *pEnvironment,
                                           OCIError *pErrorHandle,
                                       const OCIDateTime *pDateTime,
                                        ub1 *pBuffer,
                                           ub4 *pBufferLen);

sword OCIDateTimeToArray( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               const OCIDateTime *pDateTime,
                               const OCIInterval *pTimezoneInterval,
                                ub1 *pResultArray,
                                ub4 *pArrayLen,
                               ub1 fractionlSeconds);

sword OCIDateTimeFromArray( void *pEnvironment,
                                     OCIError *pErrorHandle,
                                 ub1 *pInarray,
                                 ub4 Len,
                                 ub1 type,
                                  OCIDateTime *pDateTime,
                                  const OCIInterval *pTimezoneInterval,
                                 ub1 fractionlSeconds);

sword OCIDateTimeGetTime( void *pEnvironment,
                                   OCIError *pErrorHandle,
                                     OCIDateTime *pDateTime,
                                ub1 *pHour,
                                ub1 *pMinute,
                                ub1 *pSecond,
                                ub4 *pFsec);

sword OCIDateTimeConvert( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               OCIDateTime *pSourceDate,
                                OCIDateTime *pTargetDate);




sword OCIIntervalSetYearMonth( void *pEnvironment,
                                        OCIError *pErrorHandle,
                                    sb4 year,
                                    sb4 month,
                                     OCIInterval *pResult);

sword OCIIntervalGetYearMonth( void *pEnvironment,
                                        OCIError *pErrorHandle,
                              sb4 *pYear,
                              sb4 *pMonth,
                              const OCIInterval *pResult);

sword OCIIntervalToNumber( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                const OCIInterval *pInterval,
                                 OCINumber *pNumber);

sword OCIIntervalMultiply( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                const OCIInterval *pInterval,
                                OCINumber *pNfactor,
                                 OCIInterval *pResult);

sword OCIIntervalDivide( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              OCIInterval *pDividend,
                              OCINumber *pDivisor,
                               OCIInterval *pResult);

sword OCIIntervalCompare( void *pEnvironment,
                                   OCIError *pErrorHandle,
                               OCIInterval *pInter1,
                               OCIInterval *pInter2,
                                sword *pResult);

sword OCIIntervalFromNumber( void *pEnvironment,
                                      OCIError *pErrorHandle,
                                   OCIInterval *pInterval,
                                  OCINumber *pNumber);

sword OCIIntervalSetDaySecond( void *pEnvironment,
                                        OCIError *pErrorHandle,
                                    sb4 day,
                                    sb4 hour,
                                    sb4 minute,
                                    sb4 second,
                                    sb4 fractionalSeconds,
                                     OCIInterval *pResult);

sword OCIIntervalGetDaySecond( void *pEnvironment,
                                        OCIError *pErrorHandle,
                                     sb4 *pDay,
                                     sb4 *pHour,
                                     sb4 *pMinute,
                                     sb4 *pSecond,
                                     sb4 *pFractionalSeconds,
                                    const OCIInterval *pResult);

sword OCIIntervalSubtract( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                OCIInterval *pMinuend,
                                OCIInterval *pSubtrahend,
                                 OCIInterval *pResult);

sword OCIIntervalAdd( void *pEnvironment,
                               OCIError *pErrorHandle,
                           OCIInterval *pAddend1,
                           OCIInterval *pAddend2,
                            OCIInterval *pResult);

sword OCIIntervalFromText( void *pEnvironment,
                                    OCIError *pErrorHandle,
                                const OraText *pInpstr,
                                size_t strLen,
                                 OCIInterval *pResult);

sword OCIIntervalToText( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              const OCIInterval *pInterval,
                              ub1 lfprec,
                              ub1 fractionlSeconds,
                               OraText *pBuffer,
                              size_t bufLen,
                               size_t *pResultLen);

sword OCIIntervalCheck( void *pEnvironment,
                                 OCIError *pErrorHandle,
                             const OCIInterval *pInterval,
                              ub4 *pValid);

sword OCIIntervalAssign( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              const OCIInterval *pIninter,
                               OCIInterval *pResultInterval);

sword OCIIntervalFromTZ( void *pEnvironment,
                                  OCIError *pErrorHandle,
                              const oratext *pInputString,
                              size_t strLen,
                               OCIInterval *pResult);


sword OCIConnectionPoolDestroy( OCICPool *pPoolhp,
                                         OCIError *pErrorHandle,
                                     ub4 mode);

sword OCIConnectionPoolCreate( OCIEnv *pEnvironment,
                                        OCIError *pErrorHandle,
                                        OCICPool *pPoolhp,
                                     OraText **pPoolName,
                                     sb4 *pPoolNameLen,
                                    const OraText *pDblink,
                                    sb4 dblinkLen,
                                    ub4 connMin,
                                    ub4 connMax,
                                    ub4 connIncr,
                                        const OraText *pPoolUserName,
                                    sb4 poolUserLen,
                                        const OraText *pPoolPassword,
                                    sb4 poolPassLen,
                                    ub4 mode);

sword OCISessionPoolDestroy ( OCISPool *pSpool,
                                       OCIError *pErrorHandle,
                                   ub4 mode);

sword OCISessionGet ( OCIEnv *pEnvironment,
                               OCIError *pErrorHandle,
                               OCISvcCtx **pSvchp,
                               OCIAuthInfo *pAuthhp,
                           OraText *pPoolName,
                           ub4 poolNameLen,
                           const OraText *pTagInfo,
                           ub4 tagInfoLen,
                            OraText **pRetTagInfo,
                            ub4 *pRetTagInfoLen,
                            int *pFound,
                           ub4 mode);

sword OCISessionRelease ( OCISvcCtx *pServiceContexthp,
                                   OCIError *pErrorHandle,
                               OraText *pTag,
                               ub4 tagLen,
                               ub4 mode);

sword OCISessionPoolCreate ( OCIEnv *pEnvironment,
                                      OCIError *pErrorHandle,
                                      OCISPool *pSpool,
                                   OraText **pPoolName,
                                   ub4 *pPoolNameLen,
                                  const OraText *pConnectStr,
                                  ub4 connectStrLen,
                                  ub4 sessMin,
                                  ub4 sessMax,
                                  ub4 sessIncr,
                                  OraText *pUserId,
                                  ub4 useridLen,
                                  OraText *pPassword,
                                  ub4 passwordLen,
                                  ub4 mode);

sword OCIMemStats(void *pHndlp,
                   OCIError *pErrorHandle,
                   OCIEnv **pEnvironment,
                   ub4 mode,
                   ub4 mode1,
                   oratext *pTabname);

sword OCIPing ( OCISvcCtx *pServiceContexthp,
               OCIError *pErrorHandle,
               ub4 mode);

sword OCIAppCtxClearAll(void *pSession,
                        void *pNamespaceStr,
                        ub4 nsptrLen,
                                  OCIError *pErrorHandle,
                        ub4 mode);

sword OCIAppCtxSet( void * pSession,
                         void *pNamespaceStr,
                         ub4 nsptrLen,
                         void *pAttr,
                         ub4 pAttrLen,
                         void *pValue,
                         ub4 pValueLen,
                             OCIError *pErrorHandle,
                         ub4 mode);


sword OCIKerbAttrSet( OCISession *pTrgthndlp,
                           ub4 credUse,
                           ub1 *pFtgtTIcket,
                           ub4 ticketLen,
                           ub1 *pSessionKey,
                           ub4 skeyLen,
                           ub2 ftgtKeytype,
                           ub4 ftgtTIcketFlags,
                           sb4 ftgtAuthTIme,
                           sb4 ftgtStartTIme,
                           sb4 ftgtEndTIme,
                           sb4 ftgtRenewTIme,
                           oratext *pFtgtClientPrincipal,
                           ub4 ftgtClientPrincipalLen,
                           oratext *pFtgtClientRealm,
                           ub4 ftgtClientRealmLen,
                               OCIError *pErrorHandle);


sword OCIInitEventHandle(OCIError *pErrorHandle,
                         OCIEvent *pEvent,
                         text *pStr,
                         ub4 size);

sword OCIDBShutdown(OCISvcCtx *pSvchp,
                    OCIError *pErrorHandle,
                    OCIAdmin *pAdmhp,
                    ub4 mode);

void OCIClientVersion(sword *pMajorVersion,
                      sword *pMinorVersion,
                      sword *pUpdateNum,
                      sword *pPatchNum,
                      sword *pPortUpdateNum);

struct xmlctx *OCIXmlDbInitXmlCtx( OCIEnv *pEnvironment,
                                        OCISvcCtx *pService,
                                        OCIError *pErrorHandle,
                                        ocixmldbparam *pParameters,
                                        int nParams);

sword OCIDBStartup (OCISvcCtx *pSvchp,
                    OCIError *pErrorHandle,
                    OCIAdmin *pAdmhp,
                    ub4 mode,
                    ub4 flags);

void OCIXmlDbFreeXmlCtx(struct xmlctx *xctx);



sword OCIDirPathFinish(OCIDirPathCtx *pDirectPath,
                                 OCIError *pErrorHandle);

sword OCIDirPathStreamReset(OCIDirPathStream *pDpstr,
                            OCIError *pErrorHandle);

sword OCIDirPathColArrayEntrySet(OCIDirPathColArray *pColumnArray,
                                           OCIError *pErrorHandle,
                               ub4 nRow,
                               ub2 colIdx,
                               ub1 *pCvalp,
                               ub4 cLen,
                               ub1 cflg);

sword OCIDirPathFlushRow(OCIDirPathCtx *pDirectPath,
                                   OCIError *pErrorHandle);

sword OCIDirPathColArrayEntryGet(OCIDirPathColArray *pColumnArray,
                                           OCIError *pErrorHandle,
                              ub4 nRow,
                              ub2 colIdx,
                              ub1 **pCvalpp,
                              ub4 *pClenp,
                              ub1 *pCflgp);

sword OCIDirPathColArrayToStream(OCIDirPathColArray *pColumnArray,
                                 OCIDirPathCtx *pDpctx,
                                 OCIDirPathStream *pDpstr,
                                 OCIError *pErrorHandle,
                                 ub4 rowcnt, ub4 rowoff);

sword OCIDirPathColArrayReset(OCIDirPathColArray *pColumnArray,
                                        OCIError *pErrorHandle);

sword OCIDirPathColArrayRowGet(OCIDirPathColArray *pColumnArray,
                                         OCIError *pErrorHandle,
                               ub4 nRow,
                               ub1 ***pCvalppp,
                               ub4 **pClenpp,
                               ub1 **pCflgpp);

sword OCIDirPathAbort(OCIDirPathCtx *pDirectPath,
                                OCIError *pErrorHandle);

sword OCIDirPathDataSave(OCIDirPathCtx *pDirectPath,
                                   OCIError *pErrorHandle,
                         ub4 action);

sword OCIDirPathPrepare(OCIDirPathCtx *pDirectPath,
                        OCISvcCtx *pServiceContexthp,
                                  OCIError *pErrorHandle);

sword OCIDirPathLoadStream(OCIDirPathCtx *pDirectPath,
                           OCIDirPathStream *pDpstr,
                                      OCIError *pErrorHandle);
# 23 "db2_utils.c" 2

# 1 "db2_fdw.h" 1
# 13 "db2_fdw.h"
# 1 "/products1/postgres/PG10.1/include/server/postgres_ext.h" 1
# 26 "/products1/postgres/PG10.1/include/server/postgres_ext.h"
# 1 "/products1/postgres/PG10.1/include/server/pg_config_ext.h" 1
# 27 "/products1/postgres/PG10.1/include/server/postgres_ext.h" 2




typedef unsigned int Oid;
# 47 "/products1/postgres/PG10.1/include/server/postgres_ext.h"
typedef long int pg_int64;
# 14 "db2_fdw.h" 2
# 25 "db2_fdw.h"
struct connEntry
{
  char *user;
  OCISvcCtx *svchp;
  OCISession *userhp;
  struct handleEntry *handlelist;
  int xact_level;
  struct connEntry *left;
  struct connEntry *right;
};

struct srvEntry
{
  char *connectstring;
  OCIServer *srvhp;
  struct connEntry *connlist;
  struct srvEntry *left;
  struct srvEntry *right;
};

struct envEntry
{
  char *nls_lang;
  OCIEnv *envhp;
  OCIError *errhp;
  struct srvEntry *srvlist;
  struct envEntry *left;
  struct envEntry *right;
};
typedef unsigned char DB2Text;





struct db2Session
{
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  OCIStmt *stmthp;
};

typedef struct db2Session db2Session;


typedef enum
{
  SQL_TYPE_VARCHAR,
  SQL_TYPE_CHAR,
  SQL_TYPE_SMALL,
  SQL_TYPE_INTEGER,
  SQL_TYPE_BIG,
  SQL_TYPE_DATE,
  SQL_TYPE_STAMP,
  SQL_TYPE_TIME,
  SQL_TYPE_XML,
  SQL_TYPE_BLOB,
  SQL_TYPE_CLOB,
  SQL_TYPE_DECIMAL,
  SQL_TYPE_GRAPHIC,
  SQL_TYPE_VARGRAPHIC,
  SQL_TYPE_DOUBLE,
  SQL_TYPE_FLOAT,
  SQL_TYPE_BOOLEAN,
  SQL_TYPE_OTHER
} db2Type;






struct db2Column
{
  char *name;
  db2Type db2type;
  int scale;
  char *pgname;
  int pgattnum;
  Oid pgtype;
  int pgtypmod;
  int used;
  int pkey;
  char *val;
  long val_size;
  unsigned short val_len;
  unsigned int val_len4;
  short val_null;
  int varno;
};

struct db2Table
{
  char *name;
  char *pgname;
  int ncols;
  int npgcols;
  struct db2Column **cols;
};



typedef enum
{
  BIND_STRING,
  BIND_NUMBER,
  BIND_LONG,
  BIND_LONGRAW,
  BIND_OUTPUT
} db2BindType;

struct paramDesc
{
  char *name;
  Oid type;
  db2BindType bindType;
  char *value;
  void *node;
  int colnum;
  void *bindh;
  struct paramDesc *next;
};


typedef enum
{
  FDW_ERROR,
  FDW_UNABLE_TO_ESTABLISH_CONNECTION,
  FDW_UNABLE_TO_CREATE_REPLY,
  FDW_UNABLE_TO_CREATE_EXECUTION,
  FDW_TABLE_NOT_FOUND,
  FDW_OUT_OF_MEMORY,
  FDW_SERIALIZATION_FAILURE
} db2error;




extern db2Session *db2GetSession (const char *connectstring, char *user, char *password, const char *nls_lang, const char *tablename, int curlevel);
extern void db2CloseStatement (db2Session * session);
extern void db2CloseConnections (void);
extern void db2Shutdown (void);
extern void db2Cancel (void);
extern void db2EndTransaction (void *arg, int is_commit, int silent);
extern void db2EndSubtransaction (void *arg, int nest_level, int is_commit);
extern int db2IsStatementOpen (db2Session * session);
extern struct db2Table *db2Describe (db2Session * session, char *schema, char *table, char *pgname, long max_long);
extern void db2ExplainOLD (db2Session * session, const char *query, int *nrows, char ***plan);
extern void db2PrepareQuery (db2Session * session, const char *query, const struct db2Table *db2Table, unsigned int prefetch);
extern int db2ExecuteQuery (db2Session * session, const struct db2Table *db2Table, struct paramDesc *paramList);
extern int db2FetchNext (db2Session * session);
extern void db2GetLob (db2Session * session, void *locptr, db2Type type, char **value, long *value_len, unsigned long trunc);
extern void db2ClientVersion (int *major, int *minor, int *update, int *patch, int *port_patch);
extern void db2ServerVersion (const char *connectstring, char *user, char *password, char * version, int len);
extern void *db2GetGeometryType (db2Session * session);
extern int db2GetImportColumn (db2Session * session, char *schema, char **tabname, char **colname, db2Type * type, int *charlen, int *typeprec, int *typescale, int *nullable, int *key);




extern char *db2GetShareFileName (const char *relativename);
extern void db2RegisterCallback (void *arg);
extern void db2UnregisterCallback (void *arg);
extern void *db2Alloc (size_t size);
extern void *db2Realloc (void *p, size_t size);
extern void db2Free (void *p);
extern void db2SetHandlers (void);
extern void db2Error_d (db2error sqlstate, const char *message, const char *detail, ...);
extern void db2Error (db2error sqlstate, const char *message);
extern void db2Debug2 (const char *message,...);
extern void db2Debug3 (const char *message,...);
extern void db2Debug4 (const char *message,...);
extern void db2Debug5 (const char *message,...);
# 25 "db2_utils.c" 2





static int silent = 0;



static char db2Message[2000];
static sb4 err_code;


static int oci_initialized = 0;







struct handleEntry
{
  void *handlep;
  ub4 type;
  int isDescriptor;
  struct handleEntry *next;
};




static struct envEntry *rootenvEntry = ((void *)0);
# 66 "db2_utils.c"
struct srvEntry * findsrvEntryHandle (struct srvEntry * start, OCIServer * srvhp);
struct srvEntry * findsrvEntry (struct srvEntry * start, const char * connectstring);
int deletesrvEntry(struct srvEntry * start, struct srvEntry * node);
struct srvEntry * insertsrvEntry(struct srvEntry * start, const char * connectstring, OCIServer * srvhp);
struct connEntry * findconnEntry(struct connEntry * start, const char * user);
struct connEntry * findconnEntryHandle (struct connEntry * start, OCISession * userhp);
int deleteconnEntry(struct connEntry * start, struct connEntry * node);
struct connEntry * insertconnEntry(struct connEntry * start, const char * user, OCISvcCtx *svchp,OCISession * userhp);
struct envEntry * findenvEntry(struct envEntry * start, const char * nlslang);
struct envEntry * findenvEntryHandle (struct envEntry * start, OCIEnv * envhp);
int deleteenvEntry(struct envEntry * start, struct envEntry * node);
int deleteenvEntryLang(struct envEntry * start, const char * nlslang);
struct envEntry * insertenvEntry(struct envEntry * start, const char * nlslang, OCIEnv *envhp, OCIError *errhp);
int printstruct();

static void db2SetSavepoint (db2Session * session, int nest_level);
static void setDB2Environment (char *nls_lang);
static sword checkerr (sword status, void * handle, ub4 handleType, int line, char * file);
static char *copyDB2Text (const char *string, int size, int quote);
static void closeSession (OCIEnv * envhp, OCIServer * srvhp, OCISession * userhp, int disconnect);
static void disconnectServer (OCIEnv * envhp, OCIServer * srvhp);
static void removeEnvironment (OCIEnv * envhp);
static void allocHandle (void ** handlep, ub4 type, int isDescriptor, OCIEnv * envhp, struct connEntry *connp, db2error error, const char *errmsg);
static void freeHandle (void * handlep, struct connEntry *connp);
static ub2 getDB2Type (db2Type arg);
static sb4 bind_out_callback (void *octxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 ** alenp, ub1 * piecep, void **indp, ub2 ** rcodep);
static sb4 bind_in_callback (void *ictxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 * alenp, ub1 * piecep, void **indpp);




struct srvEntry * findsrvEntryHandle (struct srvEntry * start, OCIServer * srvhp)
{
  struct srvEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (start->srvhp == srvhp) {
      return step;
    }
  }
  return ((void *)0);
}




struct srvEntry * findsrvEntry (struct srvEntry * start,const char * connectstring)
{
  struct srvEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (strcmp (step->connectstring, connectstring) == 0) {
      return step;
    }
  }
  return ((void *)0);
}



int deletesrvEntry(struct srvEntry * start, struct srvEntry * node)
{
  struct srvEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (step == node) {
      free (step->connectstring);
      step->connectstring = ((void *)0);
      if (step->left == ((void *)0) && step->right == ((void *)0)){
      } else if (step->left == ((void *)0)) {
        step->right->left = ((void *)0);
      } else if (step->right == ((void *)0)) {
        step->left->right = ((void *)0);
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
      }
      free (step);
      return 0;
    }
  }
  return 1;
}



struct srvEntry * insertsrvEntry(struct srvEntry * start,const char * connectstring, OCIServer * srvhp)
{
  struct srvEntry *step;
  struct srvEntry *new;
  if (start == ((void *)0)){
    new = malloc(sizeof(struct srvEntry));
    new->right = new->left = ((void *)0);
  } else {
    for (step = start; step->right != ((void *)0); step = step->right){ }
    new = malloc(sizeof(struct srvEntry));
    step->right = new;
    new->left = step;
    new->right = ((void *)0);
  }
  new->connectstring = strdup(connectstring);
  new->srvhp = srvhp;
  new->connlist = ((void *)0);
  return new;
}



struct connEntry * findconnEntry(struct connEntry * start, const char * user)
{
  struct connEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (strcmp (step->user, user) == 0) {
      return step;
    }
  }
  return ((void *)0);
}




struct connEntry * findconnEntryHandle (struct connEntry * start, OCISession * userhp)
{
  struct connEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (step->userhp == userhp) {
      return step;
    }
  }
  return ((void *)0);
}




int deleteconnEntry(struct connEntry * start, struct connEntry * node)
{
  struct connEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (step == node) {
      free (step->user);
      step->user = 0x00;
      if (step->left == ((void *)0) && step->right == ((void *)0)){
      } else if (step->left == ((void *)0)) {
        step->right->left = ((void *)0);
      } else if (step->right == ((void *)0)) {
        step->left->right = ((void *)0);
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
      }
      free (step);
      return 0;
    }
  }
  return 1;
}




struct connEntry * insertconnEntry(struct connEntry * start, const char * user, OCISvcCtx *svchp,OCISession * userhp)
{
  struct connEntry *step;
  struct connEntry *new;
  if (start == ((void *)0)){
    new = malloc(sizeof(struct connEntry));
    new->right = new->left = ((void *)0);
  } else {
    for (step = start; step->right != ((void *)0); step = step->right){ }
    new = malloc(sizeof(struct connEntry));
    step->right = new;
    new->left = step;
    new->right = ((void *)0);
  }
  new->user = strdup(user);
  new->handlelist = ((void *)0);
  new->xact_level = 0;
  new->svchp = svchp;
  new->userhp = userhp;
  return new;
}




struct envEntry * findenvEntry(struct envEntry * start, const char * nlslang)
{
  struct envEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (strcmp (step->nls_lang, nlslang) == 0) {
      return step;
    }
  }
  return ((void *)0);
}




struct envEntry * findenvEntryHandle (struct envEntry * start, OCIEnv * envhp)
{
  struct envEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (step->envhp == envhp) {
      return step;
    }
  }
  return ((void *)0);
}





int deleteenvEntryLang(struct envEntry * start, const char * nlslang)
{
  struct envEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (strcmp (step->nls_lang, nlslang) == 0) {
      free (step->nls_lang);
      if (step->left == ((void *)0) && step->right == ((void *)0)){
        rootenvEntry = ((void *)0);
        free (step);
        step = ((void *)0);
      } else if (step->left == ((void *)0)) {
        step->right->left = ((void *)0);
        free (step);
        step = ((void *)0);
      } else if (step->right == ((void *)0)) {
        step->left->right = ((void *)0);
        free (step);
        step = ((void *)0);
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        free (step);
        step = ((void *)0);
      }
      return 0;
    }
  }
  return 1;

}



int deleteenvEntry(struct envEntry * start, struct envEntry * node)
{
  struct envEntry *step;
  for (step = start; step != ((void *)0); step = step->right){
    if (step == node) {
      free (step->nls_lang);
      step->nls_lang = ((void *)0);
      if (step->left == ((void *)0) && step->right == ((void *)0)){
        rootenvEntry = ((void *)0);
        free (step);
        step = ((void *)0);
      } else if (step->left == ((void *)0)) {
        step->right->left = ((void *)0);
        free (step);
        step = ((void *)0);
      } else if (step->right == ((void *)0)) {
        step->left->right = ((void *)0);
        free (step);
        step = ((void *)0);
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        free (step);
        step = ((void *)0);
      }
      return 0;
    }
  }
  return 1;
}



struct envEntry * insertenvEntry(struct envEntry * start, const char * nlslang, OCIEnv *envhp, OCIError *errhp)
{
  struct envEntry *step;
  struct envEntry *new;
  if (start == ((void *)0)){
    new = malloc(sizeof(struct envEntry));
    new->right = new->left = ((void *)0);
  } else {
    for (step = start; step->right != ((void *)0); step = step->right){ }
    new = malloc(sizeof(struct envEntry));
    step->right = new;
    new->left = step;
    new->right = ((void *)0);
  }
  new->nls_lang = strdup(nlslang);
  new->envhp = envhp;
  new->errhp = errhp;
  new->srvlist = ((void *)0);
  return new;
}



int printstruct()
{
  struct envEntry *envstep;
  struct srvEntry *srvstep;
  struct connEntry *constep;
  char szBuffer[1000];
  sprintf(szBuffer,"before calling pthread_create getpid: %d getpthread_self: %lu",getpid(), pthread_self());
  db2Debug5("printstruct for: %s",szBuffer);
  for (envstep = rootenvEntry; envstep != ((void *)0); envstep = envstep->right){
    db2Debug5("EnvEntry: %x",envstep);
    db2Debug5("  nls_lang: %s",envstep->nls_lang);
    db2Debug5("  step->*envhp:%x",envstep->envhp);
    db2Debug5("  step->*errhp:%x",envstep->errhp);
    db2Debug5("  srvEntry step->*srvlist:%x",envstep->srvlist);
    db2Debug5("  step->*left:%x",envstep->left);
    db2Debug5("  step->*right:%x",envstep->right);
    for (srvstep = envstep->srvlist; srvstep != ((void *)0); srvstep = srvstep->right){
      db2Debug5("    connectstring:%s",srvstep->connectstring);
      db2Debug5("    *srvhp:%x",srvstep->srvhp);
      db2Debug5("    *connlist:%x",srvstep->connlist);
      db2Debug5("    *left:%x",srvstep->left);
      db2Debug5("    *right:%x",srvstep->right);
      for (constep = srvstep->connlist; constep != ((void *)0); constep = constep->right){
        db2Debug5("      user:%s",constep->user);
        db2Debug5("      *svchp:%x",constep->svchp);
        db2Debug5("      *userhp:%x",constep->userhp);
        db2Debug5("      *handlelist:%x",constep->handlelist);
        db2Debug5("      xact_level:%d",constep->xact_level);
        db2Debug5("      struct connEntry *left:%x",constep->left);
        db2Debug5("      struct connEntry *right:%x",constep->right);

      }
    }
  }
  return 0;
}






db2Session * db2GetSession (const char *connectstring, char *user, char *password, const char *nls_lang, const char *tablename, int curlevel)
{
  OCIEnv *envhp = ((void *)0);
  OCIError *errhp = ((void *)0);
  OCISvcCtx *svchp = ((void *)0);
  OCIServer *srvhp = ((void *)0);
  OCISession *userhp = ((void *)0);
  OCITrans *txnhp = ((void *)0);
  db2Session *session;
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  char *nlscopy = ((void *)0);
  ub4 is_connected;
  int retry;


  if (!connectstring) connectstring = "";
  if (!user) user = "";
  if (!password) password = "";
  if (!nls_lang) nls_lang = "";

  envp = findenvEntry (rootenvEntry,nls_lang);
  if (envp) {
    db2Debug2("db2GetSession found envp: %x   envhp: %x    errhp: %x",envp,envp->envhp,envp->errhp);
    envhp = envp->envhp;
    errhp = envp->errhp;
    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & srvhp, (ub4) 8, (size_t) 0, (void **) 0), (void *) envhp, 1,437, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate server handle", db2Message);

      db2Debug2("db2GetSession environment not active anymore");
      if (checkerr (OCIHandleFree( errhp, 2),(void *) envhp, 1,441, "db2_utils.c") != 0) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free error handle","%s", db2Message);
      }


      if (checkerr (OCIHandleFree( envhp, 1),(void *) envhp, 1,446, "db2_utils.c") != 0) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free environment handle","%s", db2Message);
      }
      deleteenvEntryLang(rootenvEntry, nls_lang);
      envp = ((void *)0);
    }
  }
  if (envp == ((void *)0)) {
    db2Debug2("db2GetSession create new environment");



    if ((nlscopy = strdup (nls_lang)) == ((void *)0))
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", strlen (nls_lang) + 1);


    setDB2Environment (nlscopy);


    if (checkerr (OCIEnvCreate ((OCIEnv **) & envhp, (ub4) 0x00000002, (void *) 0, (void * (*)(void *, size_t)) 0,
    (void * (*)(void *, void *, size_t)) 0, (void (*)(void *, void *)) 0, (size_t) 0, (void **) 0), (void *) envhp, 1,466, "db2_utils.c") != 0) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIEnvCreate failed to create environment handle", db2Message);
    }


    oci_initialized = 1;






    db2SetHandlers ();


    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & errhp, (ub4) 2, (size_t) 0, (void **) 0), (void *) envhp, 1,482, "db2_utils.c") != 0) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate error handle", db2Message);
    }


    envp = insertenvEntry(rootenvEntry,nlscopy,envhp,errhp);
    if (envp == ((void *)0)) {
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", sizeof (struct envEntry));
    }
    if ( rootenvEntry == ((void *)0)) {
      rootenvEntry = envp;
    }
  }


  srvp = findsrvEntry (envp->srvlist,connectstring);
  if (srvp != ((void *)0)) {
    db2Debug2("db2GetSession found connect string");
    srvhp = srvp->srvhp;

    if (checkerr (OCIAttrGet ((void *) srvhp, (ub4) 8,
         (void *) & is_connected, (ub4 *) 0, (ub4) 143, errhp), (void *) errhp, 2,504, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error connecting to DB2: OCIAttrGet failed to get connection status", db2Message);
    }

    if (is_connected == 0x0) {
      db2Debug2("db2GetSession OCI Server not connected");

      silent = 1;
      while (srvp->connlist != ((void *)0)) {
        db2Debug2("db2GetSession OCI Server not connected close Session");
 closeSession (envhp, srvhp, srvp->connlist->userhp, 0);
      }
      db2Debug2("db2GetSession OCI Server not connected disconnet");
      disconnectServer (envhp, srvhp);
      silent = 0;

      srvp = ((void *)0);
    }
  }
retry_connect:
  if (srvp == ((void *)0)) {
    db2Debug2("db2GetSession create new connect string");




    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & srvhp, (ub4) 8, (size_t) 0, (void **) 0), (void *) envhp, 1,530, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate server handle", db2Message);
    }


    if (checkerr (OCIServerAttach (srvhp, errhp, (text *) connectstring, strlen (connectstring), 0), (void *) errhp, 2,535, "db2_utils.c") != 0) {
      if (tablename)
 db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "connection for foreign table"," <%s> cannot be established. %s", tablename, db2Message);
      else
 db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot connect to foreign DB2 server", db2Message);
    }


    envp->srvlist = srvp = insertsrvEntry (envp->srvlist,connectstring,srvhp);
    if (srvp == ((void *)0)) {
      db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", sizeof (struct srvEntry));
    }
  }


  connp = findconnEntry(srvp->connlist,user);
  if (connp) {
    svchp = connp->svchp;
    userhp = connp->userhp;
  } else {
    svchp = ((void *)0);
    userhp = ((void *)0);
  }

  if (userhp == ((void *)0)) {





    db2Debug2("db2GetSession allocate service handle");
    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & svchp, (ub4) 3, (size_t) 0, (void **) 0), (void *) envhp, 1,566, "db2_utils.c") != 0) {
      free (nlscopy);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate service handle", db2Message);
    }


    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & txnhp, (ub4) 10, (size_t) 0, (void **) 0), (void *) envhp, 1,572, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate transaction handle", db2Message);
    }



    if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & userhp, (ub4) 9, (size_t) 0, (void **) 0), (void *) envhp, 1,578, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate session handle", db2Message);
    }

    if (checkerr (OCILogon (envhp,
       errhp,
       &svchp,
       (DB2Text *) user,
       strlen ((char *) user),
       (DB2Text *) password,
       strlen ((char *) password), (DB2Text *) connectstring, strlen ((char *) connectstring)), (void *) envhp, 1,588, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s ,%s", user, db2Message);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection password: %s ,%s", password, db2Message);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection connectstring: %s ,%s", connectstring, db2Message);
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server,%s", db2Message);
    }
    envp->srvlist->connlist = connp = insertconnEntry (srvp->connlist,user,svchp,userhp);



    db2RegisterCallback (connp);
  }

  if (connp->xact_level <= 0) {
    db2Debug2 ("db2_fdw: begin serializable remote transaction");


    if (checkerr (OCITransStart (svchp, errhp, (uword) 0, 0x00000400), (void *) errhp, 2,605, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCITransStart failed to start a transaction", db2Message);
      if (retry && (err_code == 1012 || err_code == 28 || err_code == 3113 || err_code == 3135)) {
        db2Debug2 ("db2_fdw: session has been terminated, try to reconnect");
        silent = 1;
        while (srvp->connlist != ((void *)0)) {
          closeSession (envhp, srvhp, srvp->connlist->userhp, 0);
        }
        disconnectServer (envhp, srvhp);
        silent = 0;
        srvp = ((void *)0);
        userhp = ((void *)0);
        retry = 0;
        goto retry_connect;
      } else {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCITransStart failed to start a transaction", db2Message);
      }
    }
    connp->xact_level = 1;
  }


  session = db2Alloc (sizeof (struct db2Session));
  session->envp = envp;
  session->srvp = srvp;
  session->connp = connp;
  session->stmthp = ((void *)0);


  db2SetSavepoint (session, curlevel);

  return session;
}





void
db2CloseStatement (db2Session * session)
{

  if (session->stmthp != ((void *)0)) {

    freeHandle (session->stmthp, session->connp);
    session->stmthp = ((void *)0);
  }
}





void
db2CloseConnections (void)
{
  while (rootenvEntry != ((void *)0)) {
    while (rootenvEntry->srvlist != ((void *)0)) {
      while (rootenvEntry->srvlist->connlist != ((void *)0)) {
 closeSession (rootenvEntry->envhp, rootenvEntry->srvlist->srvhp, rootenvEntry->srvlist->connlist->userhp, 0);
      }
      disconnectServer (rootenvEntry->envhp, rootenvEntry->srvlist->srvhp);
    }
    removeEnvironment (rootenvEntry->envhp);
  }
}






void
db2Shutdown (void)
{

  silent = 1;

  db2CloseConnections ();


  if (oci_initialized)
    (void) OCITerminate (0x00000000);
}





void
db2Cancel (void)
{
  struct envEntry *envp;
  struct srvEntry *srvp;


  for (envp = rootenvEntry; envp != ((void *)0); envp = envp->right)
    for (srvp = envp->srvlist; srvp != ((void *)0); srvp = srvp->right)
      (void) OCIBreak (srvp->srvhp, envp->errhp);
}







void
db2EndTransaction (void *arg, int is_commit, int noerror)
{
  struct connEntry *connp = ((void *)0);
  struct srvEntry *srvp = ((void *)0);
  struct envEntry *envp = ((void *)0);
  int found = 0;


  if (((struct connEntry *) arg)->xact_level == 0)
    return;


  envp = rootenvEntry;
  while (envp) {
    srvp = envp->srvlist;
    while (srvp) {
      connp = srvp->connlist;
      while (connp) {
 if (connp == (struct connEntry *) arg) {
   found = 1;
   break;
 }
 connp = connp->right;
      }
      if (found)
 break;
      srvp = srvp->right;
    }
    if (found)
      break;
    envp = envp->right;
  }

  if (!found)
    db2Error (FDW_ERROR, "db2EndTransaction internal error: handle not found in cache");


  while (connp->handlelist != ((void *)0))
    freeHandle (connp->handlelist->handlep, connp);






  if (is_commit) {
    db2Debug2 ("db2_fdw: commit remote transaction");

    if (checkerr (OCITransCommit (connp->svchp, envp->errhp, 0x00000000), (void *) envp->errhp, 2,761, "db2_utils.c") != 0 && !noerror) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error committing transaction: OCITransCommit failed", db2Message);
    }
  }
  else {
    db2Debug2 ("db2_fdw: roll back remote transaction");

    if (checkerr (OCITransRollback (connp->svchp, envp->errhp, 0x00000000), (void *) envp->errhp, 2,768, "db2_utils.c") != 0 && !noerror) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back transaction: OCITransRollback failed", db2Message);
    }
  }

  connp->xact_level = 0;
}







void
db2EndSubtransaction (void *arg, int nest_level, int is_commit)
{
  char query[50], message[60];
  struct connEntry *connp = ((void *)0);
  struct srvEntry *srvp = ((void *)0);
  struct envEntry *envp = ((void *)0);
  OCIStmt *stmthp = ((void *)0);
  int found = 0;


  if (((struct connEntry *) arg)->xact_level < nest_level)
    return;

  ((struct connEntry *) arg)->xact_level = nest_level - 1;

  if (is_commit) {




    return;
  }


  envp = rootenvEntry;
  while (envp) {
    srvp = envp->srvlist;
    while (srvp) {
      connp = srvp->connlist;
      while (connp) {
 if (connp == (struct connEntry *) arg) {
   found = 1;
   break;
 }
 connp = connp->right;
      }
      if (found)
 break;
      srvp = srvp->right;
    }
    if (found)
      break;
    envp = envp->right;
  }

  if (!found)
    db2Error (FDW_ERROR, "db2RollbackSavepoint internal error: handle not found in cache");

  snprintf (message, 59, "db2_fdw: rollback to savepoint s%d", nest_level);
  db2Debug2 (message);

  snprintf (query, 49, "ROLLBACK TO SAVEPOINT s%d", nest_level);


  allocHandle ((void **) &stmthp, 4, 0, envp->envhp, connp, FDW_OUT_OF_MEMORY, "error rolling back to savepoint: OCIHandleAlloc failed to allocate statement handle");


  if (checkerr (OCIStmtPrepare (stmthp, envp->errhp, (text *) query, (ub4) strlen (query),
    (ub4) 1, (ub4) 0x00000000), (void *) envp->errhp, 2,841, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back to savepoint: OCIStmtPrepare failed to prepare rollback statement", db2Message);
  }


  if (checkerr (OCIStmtExecute (connp->svchp, stmthp, envp->errhp, (ub4) 1, (ub4) 0,
    (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000000), (void *) envp->errhp, 2,847, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back to savepoint: OCIStmtExecute failed to set savepoint", db2Message);
  }


  freeHandle (stmthp, connp);
}





int
db2IsStatementOpen (db2Session * session)
{
  return (session->stmthp != ((void *)0));
}






struct db2Table *
db2Describe (db2Session * session, char *schema, char *table, char *pgname, long max_long)
{
  struct db2Table *reply;
  ub2 Type;
  OCIStmt *stmthp;
  OCIParam *colp;
  ub2 db2type;
  ub2 charsize;
  ub2 bin_size;
  ub1 csfrm;
  sb2 precision;
  sb1 scale;
  char *qtable, *qschema = ((void *)0), *tablename, *query;
  DB2Text *ident, *typname, *typschema;
  char *type_name, *type_schema;
  ub4 ncols, ident_size, typname_size, typschema_size;
  int i, length;


  qtable = copyDB2Text (table, strlen (table), 1);
  length = strlen (qtable);
  if (schema != ((void *)0)) {
    qschema = copyDB2Text (schema, strlen (schema), 1);
    length += strlen (qschema) + 1;
  }
  tablename = db2Alloc (length + 1);
  tablename[0] = '\0';
  if (schema != ((void *)0)) {
    strcat (tablename, qschema);
    strcat (tablename, ".");
  }
  strcat (tablename, qtable);
  db2Free (qtable);
  if (schema != ((void *)0))
    db2Free (qschema);


  length += 14;
  query = db2Alloc (length + 1);
  strcpy (query, "SELECT * FROM ");
  strcat (query, tablename);


  allocHandle ((void **) &stmthp, 4, 0, session->envp->envhp, session->connp,
        FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIHandleAlloc failed to allocate statement handle");


  if (checkerr (OCIStmtPrepare (stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
    (ub4) 1, (ub4) 0x00000000), (void *) session->envp->errhp, 2,919, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIStmtPrepare failed to prepare query", db2Message);
  }

  if (checkerr (OCIStmtExecute (session->connp->svchp, stmthp, session->envp->errhp, (ub4) 0, (ub4) 0,
    (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000010), (void *) session->envp->errhp, 2,924, "db2_utils.c") != 0) {
    if (err_code == 942)
      db2Error_d (FDW_TABLE_NOT_FOUND, "table not found",
       "DB2 table %s for foreign table \"%s\" does not exist or does not allow read access;%s,%s", tablename, pgname,
       db2Message, "DB2 table names are case sensitive (normally all uppercase).");
    else
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIStmtExecute failed to describe table", db2Message);
  }


  reply = db2Alloc (sizeof (struct db2Table));
  reply->name = tablename;
  reply->pgname = pgname;
  reply->npgcols = 0;


  if (checkerr (OCIAttrGet ((void *) stmthp, (ub4) 4,
       (void *) & ncols, (ub4 *) 0, (ub4) 18, session->envp->errhp), (void *) session->envp->errhp, 2,941, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get number of columns", db2Message);
  }

  reply->ncols = ncols;
  reply->cols = (struct db2Column **) db2Alloc (sizeof (struct db2Column *) * reply->ncols);


  for (i = 1; i <= reply->ncols; ++i) {

    reply->cols[i - 1] = (struct db2Column *) db2Alloc (sizeof (struct db2Column));
    reply->cols[i - 1]->pgname = ((void *)0);
    reply->cols[i - 1]->pgattnum = 0;
    reply->cols[i - 1]->pgtype = 0;
    reply->cols[i - 1]->pgtypmod = 0;
    reply->cols[i - 1]->used = 0;
    reply->cols[i - 1]->pkey = 0;
    reply->cols[i - 1]->val = ((void *)0);
    reply->cols[i - 1]->val_len = 0;
    reply->cols[i - 1]->val_null = 1;


    if (checkerr (OCIParamGet ((void *) stmthp, 4, session->envp->errhp, (void **) & colp, i), (void *) session->envp->errhp, 2,963, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIParamGet failed to get column data", db2Message);
    }


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & ident,
         &ident_size, (ub4) 4, session->envp->errhp), (void *) session->envp->errhp, 2,969, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column name", db2Message);
    }

    reply->cols[i - 1]->name = copyDB2Text ((char *) ident, (int) ident_size, 1);


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & db2type,
         (ub4 *) 0, (ub4) 216, session->envp->errhp), (void *) session->envp->errhp, 2,977, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type", db2Message);
    }
    Type = db2type;


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & typname,
         &typname_size, (ub4) 8, session->envp->errhp), (void *) session->envp->errhp, 2,984, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type name", db2Message);
    }


    type_name = db2Alloc (typname_size + 1);
    strncpy (type_name, (char *) typname, typname_size);
    type_name[typname_size] = '\0';


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & typschema,
         &typschema_size, (ub4) 9, session->envp->errhp), (void *) session->envp->errhp, 2,995, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column type schema name", db2Message);
    }


    type_schema = db2Alloc (typschema_size + 1);
    strncpy (type_schema, (char *) typschema, typschema_size);
    type_schema[typschema_size] = '\0';


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & csfrm,
         (ub4 *) 0, (ub4) 32, session->envp->errhp), (void *) session->envp->errhp, 2,1006, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get character set form", db2Message);
    }


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & charsize,
         (ub4 *) 0, (ub4) 285, session->envp->errhp), (void *) session->envp->errhp, 2,1012, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column length", db2Message);
    }


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & bin_size,
         (ub4 *) 0, (ub4) 1, session->envp->errhp), (void *) session->envp->errhp, 2,1018, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column size", db2Message);
    }


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & precision,
         (ub4 *) 0, (ub4) 5, session->envp->errhp), (void *) session->envp->errhp, 2,1024, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column precision", db2Message);
    }


    if (checkerr (OCIAttrGet ((void *) colp, 53, (void *) & scale,
         (ub4 *) 0, (ub4) 6, session->envp->errhp), (void *) session->envp->errhp, 2,1030, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: OCIAttrGet failed to get column scale", db2Message);
    }

    reply->cols[i - 1]->scale = scale;


    switch (Type) {
    case 96:

      reply->cols[i - 1]->db2type = SQL_TYPE_CHAR;
      reply->cols[i - 1]->val_size = bin_size * 4 + 1;
      break;
    case 1:
    case 9:

      reply->cols[i - 1]->db2type = SQL_TYPE_VARCHAR;
      reply->cols[i - 1]->val_size = bin_size * 4 + 1;
      break;
    case 113:

      reply->cols[i - 1]->db2type = SQL_TYPE_BLOB;

      reply->cols[i - 1]->val_size = sizeof (OCILobLocator *);
      break;
    case 112:

      if (csfrm == 2) {




 reply->cols[i - 1]->db2type = SQL_TYPE_OTHER;
 reply->cols[i - 1]->val_size = 0;
      }
      else {
 reply->cols[i - 1]->db2type = SQL_TYPE_CLOB;

 reply->cols[i - 1]->val_size = sizeof (OCILobLocator *);
      }
      break;
    case 3:
    case 246:

      reply->cols[i - 1]->db2type = SQL_TYPE_INTEGER;
      if (precision == 0)

 reply->cols[i - 1]->val_size = bin_size;
      else
 reply->cols[i - 1]->val_size = ((-scale) > precision ? (-scale) : precision) + 5;
      break;
    case 7:

      reply->cols[i - 1]->db2type = SQL_TYPE_INTEGER;
      if (precision == 0)

 reply->cols[i - 1]->val_size = bin_size;
      else
 reply->cols[i - 1]->val_size = ((-scale) > precision ? (-scale) : precision) + 5;
    case 4:
    case 22:

      reply->cols[i - 1]->db2type = SQL_TYPE_FLOAT;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case 12:

      reply->cols[i - 1]->db2type = SQL_TYPE_DATE;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case 187:

      reply->cols[i - 1]->db2type = SQL_TYPE_STAMP;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case 185:

      reply->cols[i - 1]->db2type = SQL_TYPE_TIME;
      reply->cols[i - 1]->val_size = bin_size;
      break;
    case 8:

      reply->cols[i - 1]->db2type = SQL_TYPE_BIG;
      reply->cols[i - 1]->val_size = max_long + 4;
      break;
    default:
      reply->cols[i - 1]->db2type = SQL_TYPE_OTHER;
      reply->cols[i - 1]->val_size = 0;
    }
  }


  freeHandle (stmthp, session->connp);

  return reply;
}







void
db2SetSavepoint (db2Session * session, int nest_level)
{
  while (session->connp->xact_level < nest_level) {
    char query[40], message[50];

    snprintf (message, 49, "db2_fdw: set savepoint s%d", session->connp->xact_level + 1);
    db2Debug2 (message);

    snprintf (query, 39, "SAVEPOINT s%d", session->connp->xact_level + 1);


    allocHandle ((void **) &(session->stmthp), 4, 0, session->envp->envhp, session->connp,
   FDW_OUT_OF_MEMORY, "error setting savepoint: OCIHandleAlloc failed to allocate statement handle");


    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
      (ub4) 1, (ub4) 0x00000000), (void *) session->envp->errhp, 2,1150, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: OCIStmtPrepare failed to prepare savepoint statement", db2Message);
    }


    if (checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
      (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1156, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: OCIStmtExecute failed to set savepoint", db2Message);
    }


    freeHandle (session->stmthp, session->connp);
    session->stmthp = ((void *)0);

    ++session->connp->xact_level;
  }
}
# 1184 "db2_utils.c"
void
setDB2Environment (char *nls_lang)
{
  if (putenv (nls_lang) != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_LANG cannot be set.");
  }


  if (putenv ("NLS_DATE_LANGUAGE=AMERICAN") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_LANGUAGE cannot be set.");
  }

  if (putenv ("NLS_DATE_FORMAT=YYYY-MM-DD HH24:MI:SS BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIMESTAMP_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_TZ_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIME_FORMAT=HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_FORMAT cannot be set.");
  }

  if (putenv ("NLS_TIME_TZ_FORMAT= HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_TZ_FORMAT cannot be set.");
  }

  if (putenv ("NLS_NUMERIC_CHARACTERS=.,") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NUMERIC_CHARACTERS cannot be set.");
  }

  if (putenv ("NLS_CALENDAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_CALENDAR cannot be set.");
  }

  if (putenv ("NLS_NCHAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NCHAR cannot be set.");
  }
}
# 1247 "db2_utils.c"
void
db2PrepareQuery (db2Session * session, const char *query, const struct db2Table *db2Table, unsigned int prefetch)
{
  int i, col_pos, is_select;
  OCIDefine *defnhp;
  static char dummy[4];
  static sb4 dummy_size = 4;
  static sb2 dummy_null;
  ub4 prefetch_rows = prefetch;


  is_select = (strncmp (query, "SELECT", 6) == 0);


  if (session->stmthp != ((void *)0)) {
    db2Error (FDW_ERROR, "db2PrepareQuery internal error: statement handle is not NULL");
  }


  allocHandle ((void **) &(session->stmthp), 4, 0, session->envp->envhp, session->connp,
        FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIHandleAlloc failed to allocate statement handle");


  if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) query, (ub4) strlen (query),
    (ub4) 1, (ub4) 0x00000000), (void *) session->envp->errhp, 2,1271, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIStmtPrepare failed to prepare remote query", db2Message);
  }


  col_pos = 0;
  for (i = 0; i < db2Table->ncols; ++i) {
    if (db2Table->cols[i]->used) {
# 1288 "db2_utils.c"
      if (is_select) {
 ub2 type;


 type = getDB2Type (db2Table->cols[i]->db2type);
 if (db2Table->cols[i]->pgtype == 2950)
   type = 5;


 if (type == 113 || type == 112) {

   allocHandle ((void **) db2Table->cols[i]->val, 50, 1, session->envp->envhp, session->connp,
         FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
 }


 defnhp = ((void *)0);
 if (checkerr (OCIDefineByPos (session->stmthp, &defnhp, session->envp->errhp, (ub4)++ col_pos,
          (void *) db2Table->cols[i]->val, (sb4) db2Table->cols[i]->val_size,
          type, (void *) & db2Table->cols[i]->val_null,
          (ub2 *) & db2Table->cols[i]->val_len, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1308, "db2_utils.c") != 0) {
   db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDefineByPos failed to define result value", db2Message);
 }

      }
      else {
 ub2 type;


 type = getDB2Type (db2Table->cols[i]->db2type);
 if (type == 113 || type == 112) {

   allocHandle ((void **) db2Table->cols[i]->val, 50, 1, session->envp->envhp, session->connp,
         FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
 }
      }
    }
  }

  if (is_select && col_pos == 0) {




    defnhp = ((void *)0);
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp, session->envp->errhp, (ub4) 1,
      (void *) dummy, dummy_size, 5, (void *) & dummy_null,
      ((void *)0), ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1335, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIDefineByPos failed to define result value", db2Message);
    }
  }


  if (checkerr (OCIAttrSet ((void *) session->stmthp, 4, (void *) & prefetch_rows, 0,
       11, session->envp->errhp), (void *) session->envp->errhp, 2,1342, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIAttrSet failed to set number of prefetched rows in statement handle", db2Message);
  }
}
# 1354 "db2_utils.c"
int
db2ExecuteQuery (db2Session * session, const struct db2Table *db2Table, struct paramDesc *paramList)
{
  sb2 *indicators;
  struct paramDesc *param;
  sword result;
  ub4 rowcount;
  int param_count = 0;
printstruct();
  for (param = paramList; param; param = param->next)
    ++param_count;


  indicators = db2Alloc (param_count * sizeof (sb2 *));


  param_count = -1;
  for (param = paramList; param; param = param->next) {
    void *value = ((void *)0);
    sb4 value_len = 0;
    ub2 value_type = 5;
    ub4 oci_mode = 0x00000000;
    OCINumber *number;
    char *num_format, *pos;

    ++param_count;
    indicators[param_count] = (sb2) ((param->value == ((void *)0)) ? -1 : 0);

    if (param->value != ((void *)0))
      switch (param->bindType) {
      case BIND_NUMBER:

 number = db2Alloc (sizeof (OCINumber));




 value_len = strlen (param->value);
 num_format = db2Alloc (value_len + 1);

 memset (num_format, '9', value_len);
 num_format[value_len] = '\0';

 if ((pos = strchr (param->value, '.')) != ((void *)0))
   num_format[pos - param->value] = 'D';

 if ((pos = strchr (param->value, 'e')) != ((void *)0))
   memset (num_format + (pos - param->value), 'E', value_len - (pos - param->value));


 if (checkerr (OCINumberFromText (session->envp->errhp, (const DB2Text *) param->value,
      (ub4) value_len, (const DB2Text *) num_format, (ub4) value_len,
      (const DB2Text *) ((void *)0), (ub4) 0, number), (void *) session->envp->errhp, 2,1406, "db2_utils.c") != 0) {
   db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCINumberFromText failed to convert parameter", db2Message);
 }
 db2Free (num_format);

 value = (void *) number;
 value_len = sizeof (OCINumber);
 value_type = 6;
 break;
      case BIND_STRING:
 value = param->value;
 value_len = strlen (param->value) + 1;
 value_type = 5;
 break;
      case BIND_LONGRAW:
 value = param->value;
 value_len = *((sb4 *) param->value) + 4;
 value_type = 95;
 break;
      case BIND_LONG:
 value = param->value;
 value_len = *((sb4 *) param->value) + 4;
 value_type = 94;
 break;
      case BIND_OUTPUT:
 value = ((void *)0);
 value_len = db2Table->cols[param->colnum]->val_size;
 value_type = getDB2Type (db2Table->cols[param->colnum]->db2type);
 if (db2Table->cols[param->colnum]->pgtype == 2950) {

   value_type = 5;
 }
 oci_mode = 0x00000002;
 break;
      }
# 1462 "db2_utils.c"
    if (checkerr (OCIBindByName (session->stmthp, (OCIBind **) & param->bindh, session->envp->errhp, (text *) param->name,
     (sb4) strlen (param->name), value, value_len, value_type,
     (void *) & indicators[param_count], ((void *)0), ((void *)0), (ub4) 0, ((void *)0), oci_mode), (void *) session->envp->errhp, 2,1464, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIBindByName failed to bind parameter", db2Message);
    }



    if (param->bindType == BIND_OUTPUT) {
      if (checkerr (OCIBindDynamic ((OCIBind *) param->bindh, session->envp->errhp,
        db2Table->cols[param->colnum], &bind_in_callback,
        db2Table->cols[param->colnum], &bind_out_callback), (void *) session->envp->errhp, 2,1473, "db2_utils.c") != 0) {
 db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIBindDynamic failed to bind callback for parameter", db2Message);
      }
    }
  }
printstruct();

  result = checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
         (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1481, "db2_utils.c");

  if (result != 0 && result != 100) {

    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIStmtExecute failed to execute remote query", db2Message);
  }


  db2Free (indicators);

  if (result == 100)
    return 0;


  if (checkerr (OCIAttrGet ((void *) session->stmthp, (ub4) 4,
       (void *) & rowcount, (ub4 *) 0, (ub4) 9, session->envp->errhp), (void *) session->envp->errhp, 2,1496, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: OCIAttrGet failed to get number of affected rows", db2Message);
  }


  for (param = paramList; param; param = param->next)
    if (param->bindType == BIND_OUTPUT) {





      db2Table->cols[param->colnum]->val_len = (unsigned short) db2Table->cols[param->colnum]->val_len4;

    }

  return rowcount;
}





int
db2FetchNext (db2Session * session)
{
  sword result;


  if (session->stmthp == ((void *)0)) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }


  result = checkerr (OCIStmtFetch2 (session->stmthp, session->envp->errhp, 1, 0x00000002, 0, 0x00000000), (void *) session->envp->errhp, 2,1530, "db2_utils.c");

  if (result != 0 && result != 100) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: OCIStmtFetch2 failed to fetch next result row", db2Message);
  }

  return (result == 0);
}






void
db2GetLob (db2Session * session, void *locptr, db2Type type, char **value, long *value_len, unsigned long trunc)
{
  OCILobLocator *locp = *(OCILobLocator **) locptr;
  ub4 amount_char= 4096000000;
  sword result = 0;
  ub4 resultlob;


  *value_len = 0;


  do {

    if (*value_len == 0)
      *value = db2Alloc (8132 + 1);
    else
      *value = db2Realloc (*value, *value_len + 8132 + 1);






    resultlob = OCILobRead (
                  session->connp->svchp,
                  session->envp->errhp,
                  locp,
                  &amount_char,
                  (ub4) 1,
                  (void *) (*value + *value_len),
                  (ub4) 8132,
                  (void *) 0,
                  ((void *)0),
                  (ub2) 0,
                  (ub1) 1
                  );
    result = checkerr (resultlob,(void *) session->envp->errhp, 2,1581, "db2_utils.c");

    if (result == -1) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: OCILobRead failed to read LOB chunk", db2Message);
    }


    *value_len += (long) amount_char;
  }
  while (result == 99);


  (*value)[*value_len] = '\0';
}





void
db2ClientVersion (int *major, int *minor, int *update, int *patch, int *port_patch)
{
  OCIClientVersion (major, minor, update, patch, port_patch);
}





void
db2ServerVersion (const char *connectstring, char *user, char *password, char * version, int len)
{
  OraText vers[len];
  OCIEnv *envhp = ((void *)0);
  OCIError *errhp = ((void *)0);
  OCISvcCtx *svchp = ((void *)0);


  memset (vers,0x00,len);

  if (checkerr (OCIEnvCreate ((OCIEnv **) & envhp, (ub4) 0x00000002,
        (void *) 0, (void * (*)(void *, size_t)) 0,
        (void * (*)(void *, void *, size_t)) 0, (void (*)(void *, void *)) 0, (size_t) 0, (void **) 0), (void *) envhp, 1,1623, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIEnvCreate failed to create environment handle", db2Message);
  }

  if (checkerr (OCIHandleAlloc ((void *) envhp, (void **) & errhp, (ub4) 2, (size_t) 0, (void **) 0), (void *) envhp, 1,1627, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: OCIHandleAlloc failed to allocate error handle", db2Message);
  }

  if (checkerr (OCILogon (envhp,
                            errhp,
                            &svchp,
                            (DB2Text *) user,
                            strlen ((char *) user),
                            (DB2Text *) password,
                            strlen ((char *) password), (DB2Text *) connectstring, strlen ((char *) connectstring)), (void *) envhp, 1,1637, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s %s", user, db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server %s", db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection Password: %s %s", password, db2Message);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection Connectstring: %s %s", connectstring, db2Message);
  }


  if (checkerr (OCIServerVersion (svchp, errhp, vers, len, 3 ), (void *) errhp, 2,1645, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error getting server version:"," OCIServerVersion failed to retrieve version", db2Message);
  }

  strcpy (version,(char *)vers);

  if (checkerr ( OCILogoff( svchp, errhp ), (void *) envhp, 1,1651, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot Logoff from DB2 server","%s", db2Message);
  }


  if (checkerr (OCIHandleFree( svchp, 3 ), (void *) envhp, 1,1656, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free connection Handle","%s", db2Message);
  }


  if (checkerr (OCIHandleFree( errhp, 2),(void *) envhp, 1,1661, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free error handle","%s", db2Message);
  }


  if (checkerr (OCIHandleFree( envhp, 1),(void *) envhp, 1,1666, "db2_utils.c") != 0) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot free environment handle","%s", db2Message);
  }

  (void)OCITerminate( 0x00000000 );
}






int
db2GetImportColumn (db2Session * session, char *schema, char **tabname, char **colname, db2Type * type, int *charlen, int *typeprec, int *typescale, int *nullable, int *key)
{

  static char s_tabname[129], s_colname[129];
  char typename[129] = { '\0' }, isnull[2] = { '\0'};
  int count = 0;
  const char *const schema_query = "select count(*) from SYSIBM.SYSSCHEMATA where name=:nsp";
  const char *const column_query =
    "select A.TABLE_NAME,B.NAME,COLTYPE,LENGTH,SCALE,COLNO \n"
    "from sysibm.tables a,sysibm.SYSCOLUMNS b \n"
    "where table_schema=:nsp and (TABLE_TYPE like 'BASE TABLE%' OR TABLE_TYPE like 'VIEW') and A.TABLE_NAME = B.TBNAME \n"
    "order by TABLE_NAME,COLNO";
  OCIBind *bndhp = ((void *)0);
  sb2 ind = 0, ind_tabname, ind_colname, ind_typename, ind_charlen = (OCIInd)0, ind_precision = (OCIInd)0, ind_scale = (OCIInd)0, ind_isnull, ind_key;
  OCIDefine *defnhp_tabname = ((void *)0), *defnhp_colname = ((void *)0), *defnhp_typename = ((void *)0),
    *defnhp_charlen = ((void *)0), *defnhp_scale = ((void *)0), *defnhp_isnull = ((void *)0), *defnhp_key = ((void *)0), *defnhp_count = ((void *)0);
  ub2 len_tabname, len_colname, len_typename, len_charlen, len_scale, len_isnull, len_key, len_count;
  ub4 prefetch_rows = 200;
  sword result;


  *tabname = s_tabname;
  *colname = s_colname;


  if (session->stmthp == ((void *)0)) {

    allocHandle ((void **) &(session->stmthp), 4, 0, session->envp->envhp, session->connp,
   FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIHandleAlloc failed to allocate statement handle");


    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) schema_query, (ub4) strlen (schema_query),
      (ub4) 1, (ub4) 0x00000000), (void *) session->envp->errhp, 2,1711, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtPrepare failed to prepare schema query", db2Message);
    }


    if (checkerr (OCIBindByName (session->stmthp, &bndhp, session->envp->errhp, (text *) ":nsp",
     (sb4) 4, (void *) schema, (sb4) (strlen (schema) + 1),
     5, (void *) & ind, ((void *)0), ((void *)0), (ub4) 0, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1718, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIBindByName failed to bind parameter", db2Message);
    }


    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_count, session->envp->errhp, (ub4) 1,
      (void *) & count, (sb4) sizeof (int),
      3, (void *) & ind, (ub2 *) & len_count, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1725, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result", db2Message);
    }


    if (checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
      (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1731, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtExecute failed to execute schema query", db2Message);
    }


    freeHandle (session->stmthp, session->connp);
    session->stmthp = ((void *)0);


    if (count == 0)
      return -1;
  }

  if (session->stmthp == ((void *)0)) {

    allocHandle ((void **) &(session->stmthp), 4, 0, session->envp->envhp, session->connp,
   FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIHandleAlloc failed to allocate statement handle");


    if (checkerr (OCIAttrSet ((void *) session->stmthp, 4, (void *) & prefetch_rows, 0,
         11, session->envp->errhp), (void *) session->envp->errhp, 2,1751, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIAttrSet failed to set number of prefetched rows in statement handle", db2Message);
    }


    if (checkerr (OCIStmtPrepare (session->stmthp, session->envp->errhp, (text *) column_query, (ub4) strlen (column_query),
      (ub4) 1, (ub4) 0x00000000), (void *) session->envp->errhp, 2,1757, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtPrepare failed to prepare remote query", db2Message);
    }


    if (checkerr (OCIBindByName (session->stmthp, &bndhp, session->envp->errhp, (text *) ":nsp", (sb4) 4, (void *) schema, (sb4) (strlen (schema) + 1),
     5, (void *) & ind, ((void *)0), ((void *)0), (ub4) 0, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1763, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIBindByName failed to bind parameter", db2Message);
    }


    s_tabname[128] = '\0';
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_tabname, session->envp->errhp, (ub4) 1, (void *) s_tabname, (sb4) 129,
      5, (void *) & ind_tabname, (ub2 *) & len_tabname, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1770, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for table name", db2Message);
    }

    s_colname[128] = '\0';
    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_colname, session->envp->errhp, (ub4) 2, (void *) s_colname, (sb4) 129,
      5, (void *) & ind_colname, (ub2 *) & len_colname, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1776, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for column name", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_typename, session->envp->errhp, (ub4) 3, (void *) typename, (sb4) 129,
      5, (void *) & ind_typename, (ub2 *) & len_typename, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1781, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for type name", db2Message);
    }


    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_charlen, session->envp->errhp, (ub4) 4, (void *) charlen, (sb4) sizeof (int),
      3, (void *) & ind_charlen, (ub2 *) & len_charlen, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1787, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for character length", db2Message);
    }


    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_scale, session->envp->errhp, (ub4) 5, (void *) typescale, (sb4) sizeof (int),
      3, (void *) & ind_scale, (ub2 *) & len_scale, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1793, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for type scale", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_isnull, session->envp->errhp, (ub4) 6, (void *) isnull, (sb4) 2,
      5, (void *) & ind_isnull, (ub2 *) & len_isnull, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1798, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for nullability", db2Message);
    }

    if (checkerr (OCIDefineByPos (session->stmthp, &defnhp_key, session->envp->errhp, (ub4) 7, (void *) key, (sb4) sizeof (int),
      3, (void *) & ind_key, (ub2 *) & len_key, ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1803, "db2_utils.c") != 0) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIDefineByPos failed to define result for primary key", db2Message);
    }


    result = checkerr (OCIStmtExecute (session->connp->svchp, session->stmthp, session->envp->errhp, (ub4) 1, (ub4) 0,
           (const OCISnapshot *) ((void *)0), (OCISnapshot *) ((void *)0), 0x00000000), (void *) session->envp->errhp, 2,1809, "db2_utils.c");

    if (result != 0 && result != 100) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtExecute failed to execute column query", db2Message);
    }
  }
  else {

    result = checkerr (OCIStmtFetch2 (session->stmthp, session->envp->errhp, 1, 0x00000002, 0, 0x00000000), (void *) session->envp->errhp, 2,1817, "db2_utils.c");

    if (result != 0 && result != 100) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: OCIStmtFetch2 failed to fetch next result row", db2Message);
    }
  }

  if (result == 100) {

    freeHandle (session->stmthp, session->connp);
    session->stmthp = ((void *)0);

    return 0;
  }
  else {

    *nullable = (isnull[0] == 'Y');


    if (strcmp (typename, "VARCHAR ") == 0)
      *type = SQL_TYPE_VARCHAR;
    else if (strcmp (typename, "CHAR    ") == 0)
      *type = SQL_TYPE_CHAR;
    else if (strcmp (typename, "SMALLINT") == 0)
      *type = SQL_TYPE_SMALL;
    else if (strcmp (typename, "INTEGER ") == 0)
      *type = SQL_TYPE_INTEGER;
    else if (strcmp (typename, "BIGINT  ") == 0)
      *type = SQL_TYPE_BIG;
    else if (strcmp (typename, "DATE    ") == 0)
      *type = SQL_TYPE_DATE;
    else if (strcmp (typename, "TIMESTMP") == 0)
      *type = SQL_TYPE_STAMP;
    else if (strcmp (typename, "TIME    ") == 0)
      *type = SQL_TYPE_TIME;
    else if (strcmp (typename, "XML     ") == 0)
      *type = SQL_TYPE_XML;
    else if (strcmp (typename, "BLOB    ") == 0)
      *type = SQL_TYPE_BLOB;
    else if (strcmp (typename, "CLOB    ") == 0)
      *type = SQL_TYPE_CLOB;
    else if (strcmp (typename, "DECIMAL ") == 0)
      *type = SQL_TYPE_DECIMAL;
    else if (strcmp (typename, "GRAPHIC ") == 0)
      *type = SQL_TYPE_GRAPHIC;
    else if (strcmp (typename, "VARGRAPH") == 0)
      *type = SQL_TYPE_VARGRAPHIC;
    else if (strcmp (typename, "DOUBLE  ") == 0)
      *type = SQL_TYPE_DOUBLE;
    else if (strcmp (typename, "FLOAT   ") == 0)
      *type = SQL_TYPE_FLOAT;
    else if (strcmp (typename, "BOOLEAN ") == 0)
      *type = SQL_TYPE_BOOLEAN;
    else
      *type = SQL_TYPE_OTHER;


    if (ind_charlen != (OCIInd)0)
      *charlen = 0;
    if (ind_precision != (OCIInd)0)
      *typeprec = 0;
    if (ind_scale != (OCIInd)0)
      *typescale = 0;
  }

  return 1;
}





sword checkerr (sword status, void * handle, ub4 handleType,int line, char * file)
{
  char message[1024 + 1];
  char submessage[200];
  char sqlstate[5 + 1];
  sb4 sqlcode;
  ub4 i = 1;


  memset (db2Message,0x00,sizeof(db2Message));
  switch (status)
  {
    case 0:
      break;
    case -2:
      sprintf(db2Message,"-CI INVALID HANDLE-----\nline=%d\nfile=%s\n",line,file);
      break;
    case -1:
      memset (submessage,0x00,sizeof(submessage));
      memset (message,0x00,sizeof(message));
      while (OCIErrorGet ( handle, i, (text *)sqlstate, &sqlcode, (text *) message, sizeof(message), handleType) == 0) {
        sprintf(submessage,"SQLSTATE = %s  SQLCODE = %d\nline=%d\nfile=%s\n", sqlstate,sqlcode,line,file);
        if ((sizeof(db2Message) - strlen(db2Message))> strlen(submessage)+1){
          strcat (db2Message,submessage);
        }
        if ((sizeof(db2Message) - strlen(db2Message))> strlen(message)+2){
          strcat (db2Message,message);
          strcat (db2Message,"\n");
        }
        i++;
        memset (submessage,0x00,sizeof(submessage));
        memset (message,0x00,sizeof(message));
      }
      break;
    case 1:
      status = 0;
      break;
    case 99:
      break;
    case 100:
      strcpy (db2Message, "SQL0100: no data found");
      err_code = (sb4) 100;
      break;
    default:
      break;
  }

  return status;
}







char * copyDB2Text (const char *string, int size, int quote)
{
  int resultsize = (quote ? size + 2 : size);
  register int i, j = -1;
  char *result;


  if (string[0] == '(' && string[size - 1] == ')') {
    result = db2Alloc (size + 1);
    memcpy (result, string, size);
    result[size] = '\0';
    return result;
  }

  if (quote) {
    for (i = 0; i < size; ++i) {
      if (string[i] == '"')
 ++resultsize;
    }
  }

  result = db2Alloc (resultsize + 1);
  if (quote)
    result[++j] = '"';
  for (i = 0; i < size; ++i) {
    result[++j] = string[i];
    if (quote && string[i] == '"')
      result[++j] = '"';
  }
  if (quote)
    result[++j] = '"';
  result[j + 1] = '\0';

  return result;
}






void closeSession (OCIEnv * envhp, OCIServer * srvhp, OCISession * userhp, int disconnect)
{
  struct envEntry *envp;
  struct srvEntry *srvp;
  struct connEntry *connp;
  OCITrans *txnhp = ((void *)0);


  envp = findenvEntryHandle (rootenvEntry,envhp);

  if (envp == ((void *)0)) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: environment handle not found in cache");
  }


  srvp = findsrvEntryHandle (envp->srvlist,srvhp);

  if (srvp == ((void *)0)) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: server handle not found in cache");
  }


  connp = findconnEntryHandle (srvp->connlist,userhp);

  if (connp == ((void *)0)) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: user handle not found in cache");
  }


  if (checkerr (OCISessionEnd (connp->svchp, envp->errhp, connp->userhp, 0x00000000), (void *) envp->errhp, 2,2018, "db2_utils.c") != 0 && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCISessionEnd failed to terminate session", db2Message);
  }


  (void) OCIHandleFree ((void *) connp->userhp, 9);


  if (checkerr (OCIAttrGet ((void *) connp->svchp, (ub4) 3,
       (void *) & txnhp, (ub4 *) 0, (ub4) 8, envp->errhp), (void *) envp->errhp, 2,2027, "db2_utils.c") != 0 && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCIAttrGet failed to get transaction handle", db2Message);
  }


  (void) OCIHandleFree ((void *) connp->svchp, 3);


  (void) OCIHandleFree ((void *) txnhp, 10);


  deleteconnEntry(srvp->connlist, connp);
  srvp->connlist = ((void *)0);


  if (disconnect && srvp->left->connlist == ((void *)0))
    disconnectServer (envhp, srvhp);


  db2UnregisterCallback (connp);




}





void disconnectServer (OCIEnv * envhp, OCIServer * srvhp)
{
  struct envEntry *envp;
  struct srvEntry *srvp;


  envp = findenvEntryHandle (rootenvEntry,envhp);
  if (envp == ((void *)0)) {
    if (silent) return;
    else db2Error (FDW_ERROR, "disconnectServer internal error: environment handle not found in cache");
  }


  srvp = findsrvEntryHandle (envp->srvlist,srvhp);

  if (srvp == ((void *)0)) {
    if (silent) return;
    else db2Error (FDW_ERROR, "disconnectServer internal error: server handle not found in cache");
  }


  if (checkerr (OCIServerDetach (srvp->srvhp, envp->errhp, 0x00000000), (void *) envp->errhp, 2,2078, "db2_utils.c") != 0 && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: OCIServerDetach failed to detach from server", db2Message);
  }


  (void) OCIHandleFree ((void *) srvp->srvhp, 8);


  deletesrvEntry(envp->srvlist,srvp);
  envp->srvlist = ((void *)0);
}





void removeEnvironment (OCIEnv * envhp)
{
  struct envEntry *envp;


  envp = findenvEntryHandle (rootenvEntry,envhp);

  if (envp == ((void *)0)) {
    if (silent)
      return;
    else
      db2Error (FDW_ERROR, "removeEnvironment internal error: environment handle not found in cache");
  }


  (void) OCIHandleFree ((void *) envp->errhp, 2);


  (void) OCIHandleFree ((void *) envp->envhp, 1);


  deleteenvEntry(rootenvEntry,envp);
  envp = ((void *)0);
}






void allocHandle (void ** handlepp, ub4 type, int isDescriptor, OCIEnv * envhp, struct connEntry *connp, db2error error, const char *errmsg)
{
  struct handleEntry *entry;
  sword rc;
  printstruct();


  if ((entry = malloc (sizeof (struct handleEntry))) == ((void *)0)) {
    db2Error_d (FDW_OUT_OF_MEMORY, "error allocating handle:"," failed to allocate %d bytes of memory", sizeof (struct handleEntry));
  }

  if (isDescriptor)
    rc = OCIDescriptorAlloc ((const void *) envhp, handlepp, type, (size_t) 0, ((void *)0));
  else
    rc = OCIHandleAlloc ((const void *) envhp, handlepp, type, (size_t) 0, ((void *)0));

  if (rc != 0) {
    free (entry);
    db2Error (error, errmsg);
  }


  entry->handlep = *handlepp;
  entry->type = type;
  entry->isDescriptor = isDescriptor;
  entry->next = connp->handlelist;
  connp->handlelist = entry;
}






void
freeHandle (void * handlep, struct connEntry *connp)
{
  struct handleEntry *entry, *preventry = ((void *)0);


  for (entry = connp->handlelist; entry != ((void *)0); entry = entry->next) {
    if (entry->handlep == handlep)
      break;

    preventry = entry;
  }

  if (entry == ((void *)0))
    db2Error (FDW_ERROR, "internal error freeing handle: not found in cache");


  if (entry->isDescriptor)
    (void) OCIDescriptorFree (handlep, entry->type);
  else
    (void) OCIHandleFree (handlep, entry->type);


  if (preventry == ((void *)0))
    connp->handlelist = entry->next;
  else
    preventry->next = entry->next;

  free (entry);
}






ub2
getDB2Type (db2Type arg)
{
  switch (arg) {
  case SQL_TYPE_BLOB:
    return 113;
  case SQL_TYPE_CLOB:
    return 112;
  case SQL_TYPE_BIG:
    return 94;
  default:

    return 5;
  }
}






sb4
bind_out_callback (void *octxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 ** alenp, ub1 * piecep, void **indp, ub2 ** rcodep)
{
  struct db2Column *column = (struct db2Column *) octxp;

  if (column->db2type == SQL_TYPE_BLOB || column->db2type == SQL_TYPE_CLOB) {

    *bufpp = *((OCILobLocator **) column->val);
    *indp = &(column->val_null);
  }
  else {

    *bufpp = column->val;
    *indp = &(column->val_null);
  }
  column->val_len4 = (unsigned int) column->val_size;
  *alenp = &(column->val_len4);
  *rcodep = ((void *)0);

  if (*piecep == 0)
    return -24200;
  else
    return -1;
}







sb4
bind_in_callback (void *ictxp, OCIBind * bindp, ub4 iter, ub4 index, void **bufpp, ub4 * alenp, ub1 * piecep, void **indpp)
{
  struct db2Column *column = (struct db2Column *) ictxp;

  *piecep = 0;

  column->val_null = -1;
  *indpp = &(column->val_null);

  return -24200;
}
