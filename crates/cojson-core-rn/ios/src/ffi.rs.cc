#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#if __cplusplus >= 201703L
#include <string_view>
#endif

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wmissing-declarations"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wdollar-in-identifier-extension"
#endif // __clang__
#endif // __GNUC__

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

namespace {
template <typename T>
class impl;
} // namespace

class String;

#ifndef CXXBRIDGE1_RUST_STR
#define CXXBRIDGE1_RUST_STR
class Str final {
public:
  Str() noexcept;
  Str(const String &) noexcept;
  Str(const std::string &);
  Str(const char *);
  Str(const char *, std::size_t);

  Str &operator=(const Str &) & noexcept = default;

  explicit operator std::string() const;
#if __cplusplus >= 201703L
  explicit operator std::string_view() const;
#endif

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  Str(const Str &) noexcept = default;
  ~Str() noexcept = default;

  using iterator = const char *;
  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const Str &) const noexcept;
  bool operator!=(const Str &) const noexcept;
  bool operator<(const Str &) const noexcept;
  bool operator<=(const Str &) const noexcept;
  bool operator>(const Str &) const noexcept;
  bool operator>=(const Str &) const noexcept;

  void swap(Str &) noexcept;

private:
  class uninit;
  Str(uninit) noexcept;
  friend impl<Str>;

  std::array<std::uintptr_t, 2> repr;
};
#endif // CXXBRIDGE1_RUST_STR

#ifndef CXXBRIDGE1_RUST_BOX
#define CXXBRIDGE1_RUST_BOX
template <typename T>
class Box final {
public:
  using element_type = T;
  using const_pointer =
      typename std::add_pointer<typename std::add_const<T>::type>::type;
  using pointer = typename std::add_pointer<T>::type;

  Box() = delete;
  Box(Box &&) noexcept;
  ~Box() noexcept;

  explicit Box(const T &);
  explicit Box(T &&);

  Box &operator=(Box &&) & noexcept;

  const T *operator->() const noexcept;
  const T &operator*() const noexcept;
  T *operator->() noexcept;
  T &operator*() noexcept;

  template <typename... Fields>
  static Box in_place(Fields &&...);

  void swap(Box &) noexcept;

  static Box from_raw(T *) noexcept;

  T *into_raw() noexcept;

  /* Deprecated */ using value_type = element_type;

private:
  class uninit;
  class allocation;
  Box(uninit) noexcept;
  void drop() noexcept;

  friend void swap(Box &lhs, Box &rhs) noexcept { lhs.swap(rhs); }

  T *ptr;
};

template <typename T>
class Box<T>::uninit {};

template <typename T>
class Box<T>::allocation {
  static T *alloc() noexcept;
  static void dealloc(T *) noexcept;

public:
  allocation() noexcept : ptr(alloc()) {}
  ~allocation() noexcept {
    if (this->ptr) {
      dealloc(this->ptr);
    }
  }
  T *ptr;
};

template <typename T>
Box<T>::Box(Box &&other) noexcept : ptr(other.ptr) {
  other.ptr = nullptr;
}

template <typename T>
Box<T>::Box(const T &val) {
  allocation alloc;
  ::new (alloc.ptr) T(val);
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::Box(T &&val) {
  allocation alloc;
  ::new (alloc.ptr) T(std::move(val));
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::~Box() noexcept {
  if (this->ptr) {
    this->drop();
  }
}

template <typename T>
Box<T> &Box<T>::operator=(Box &&other) & noexcept {
  if (this->ptr) {
    this->drop();
  }
  this->ptr = other.ptr;
  other.ptr = nullptr;
  return *this;
}

template <typename T>
const T *Box<T>::operator->() const noexcept {
  return this->ptr;
}

template <typename T>
const T &Box<T>::operator*() const noexcept {
  return *this->ptr;
}

template <typename T>
T *Box<T>::operator->() noexcept {
  return this->ptr;
}

template <typename T>
T &Box<T>::operator*() noexcept {
  return *this->ptr;
}

template <typename T>
template <typename... Fields>
Box<T> Box<T>::in_place(Fields &&...fields) {
  allocation alloc;
  auto ptr = alloc.ptr;
  ::new (ptr) T{std::forward<Fields>(fields)...};
  alloc.ptr = nullptr;
  return from_raw(ptr);
}

template <typename T>
void Box<T>::swap(Box &rhs) noexcept {
  using std::swap;
  swap(this->ptr, rhs.ptr);
}

template <typename T>
Box<T> Box<T>::from_raw(T *raw) noexcept {
  Box box = uninit{};
  box.ptr = raw;
  return box;
}

template <typename T>
T *Box<T>::into_raw() noexcept {
  T *raw = this->ptr;
  this->ptr = nullptr;
  return raw;
}

template <typename T>
Box<T>::Box(uninit) noexcept {}
#endif // CXXBRIDGE1_RUST_BOX

#ifndef CXXBRIDGE1_RUST_ERROR
#define CXXBRIDGE1_RUST_ERROR
class Error final : public std::exception {
public:
  Error(const Error &);
  Error(Error &&) noexcept;
  ~Error() noexcept override;

  Error &operator=(const Error &) &;
  Error &operator=(Error &&) & noexcept;

  const char *what() const noexcept override;

private:
  Error() noexcept = default;
  friend impl<Error>;
  const char *msg;
  std::size_t len;
};
#endif // CXXBRIDGE1_RUST_ERROR

#ifndef CXXBRIDGE1_RUST_OPAQUE
#define CXXBRIDGE1_RUST_OPAQUE
class Opaque {
public:
  Opaque() = delete;
  Opaque(const Opaque &) = delete;
  ~Opaque() = delete;
};
#endif // CXXBRIDGE1_RUST_OPAQUE

#ifndef CXXBRIDGE1_IS_COMPLETE
#define CXXBRIDGE1_IS_COMPLETE
namespace detail {
namespace {
template <typename T, typename = std::size_t>
struct is_complete : std::false_type {};
template <typename T>
struct is_complete<T, decltype(sizeof(T))> : std::true_type {};
} // namespace
} // namespace detail
#endif // CXXBRIDGE1_IS_COMPLETE

#ifndef CXXBRIDGE1_LAYOUT
#define CXXBRIDGE1_LAYOUT
class layout {
  template <typename T>
  friend std::size_t size_of();
  template <typename T>
  friend std::size_t align_of();
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return T::layout::size();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return sizeof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      size_of() {
    return do_size_of<T>();
  }
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return T::layout::align();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return alignof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      align_of() {
    return do_align_of<T>();
  }
};

template <typename T>
std::size_t size_of() {
  return layout::size_of<T>();
}

template <typename T>
std::size_t align_of() {
  return layout::align_of<T>();
}
#endif // CXXBRIDGE1_LAYOUT

namespace repr {
struct PtrLen final {
  void *ptr;
  ::std::size_t len;
};
} // namespace repr

namespace detail {
template <typename T, typename = void *>
struct operator_new {
  void *operator()(::std::size_t sz) { return ::operator new(sz); }
};

template <typename T>
struct operator_new<T, decltype(T::operator new(sizeof(T)))> {
  void *operator()(::std::size_t sz) { return T::operator new(sz); }
};
} // namespace detail

template <typename T>
union MaybeUninit {
  T value;
  void *operator new(::std::size_t sz) { return detail::operator_new<T>{}(sz); }
  MaybeUninit() {}
  ~MaybeUninit() {}
};

namespace {
template <>
class impl<Error> final {
public:
  static Error error(repr::PtrLen repr) noexcept {
    Error error;
    error.msg = static_cast<char const *>(repr.ptr);
    error.len = repr.len;
    return error;
  }
};
} // namespace
} // namespace cxxbridge1
} // namespace rust

namespace craby {
  namespace cojsoncorern {
    namespace bridging {
      struct CojsonCoreRn;
    }
  }
}

namespace craby {
namespace cojsoncorern {
namespace bridging {
#ifndef CXXBRIDGE1_STRUCT_craby$cojsoncorern$bridging$CojsonCoreRn
#define CXXBRIDGE1_STRUCT_craby$cojsoncorern$bridging$CojsonCoreRn
struct CojsonCoreRn final : public ::rust::Opaque {
  ~CojsonCoreRn() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_craby$cojsoncorern$bridging$CojsonCoreRn

extern "C" {
::std::size_t craby$cojsoncorern$bridging$cxxbridge1$CojsonCoreRn$operator$sizeof() noexcept;
::std::size_t craby$cojsoncorern$bridging$cxxbridge1$CojsonCoreRn$operator$alignof() noexcept;

::craby::cojsoncorern::bridging::CojsonCoreRn *craby$cojsoncorern$bridging$cxxbridge1$create_cojson_core_rn(::std::size_t id, ::rust::Str data_path) noexcept;

::rust::repr::PtrLen craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_add(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b, double *return$) noexcept;

::rust::repr::PtrLen craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_divide(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b, double *return$) noexcept;

::rust::repr::PtrLen craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_multiply(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b, double *return$) noexcept;

::rust::repr::PtrLen craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_subtract(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b, double *return$) noexcept;
} // extern "C"

::std::size_t CojsonCoreRn::layout::size() noexcept {
  return craby$cojsoncorern$bridging$cxxbridge1$CojsonCoreRn$operator$sizeof();
}

::std::size_t CojsonCoreRn::layout::align() noexcept {
  return craby$cojsoncorern$bridging$cxxbridge1$CojsonCoreRn$operator$alignof();
}

::rust::Box<::craby::cojsoncorern::bridging::CojsonCoreRn> createCojsonCoreRn(::std::size_t id, ::rust::Str data_path) noexcept {
  return ::rust::Box<::craby::cojsoncorern::bridging::CojsonCoreRn>::from_raw(craby$cojsoncorern$bridging$cxxbridge1$create_cojson_core_rn(id, data_path));
}

double add(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b) {
  ::rust::MaybeUninit<double> return$;
  ::rust::repr::PtrLen error$ = craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_add(it_, a, b, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

double divide(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b) {
  ::rust::MaybeUninit<double> return$;
  ::rust::repr::PtrLen error$ = craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_divide(it_, a, b, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

double multiply(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b) {
  ::rust::MaybeUninit<double> return$;
  ::rust::repr::PtrLen error$ = craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_multiply(it_, a, b, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

double subtract(::craby::cojsoncorern::bridging::CojsonCoreRn &it_, double a, double b) {
  ::rust::MaybeUninit<double> return$;
  ::rust::repr::PtrLen error$ = craby$cojsoncorern$bridging$cxxbridge1$cojson_core_rn_subtract(it_, a, b, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}
} // namespace bridging
} // namespace cojsoncorern
} // namespace craby

extern "C" {
::craby::cojsoncorern::bridging::CojsonCoreRn *cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$alloc() noexcept;
void cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$dealloc(::craby::cojsoncorern::bridging::CojsonCoreRn *) noexcept;
void cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$drop(::rust::Box<::craby::cojsoncorern::bridging::CojsonCoreRn> *ptr) noexcept;
} // extern "C"

namespace rust {
inline namespace cxxbridge1 {
template <>
::craby::cojsoncorern::bridging::CojsonCoreRn *Box<::craby::cojsoncorern::bridging::CojsonCoreRn>::allocation::alloc() noexcept {
  return cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$alloc();
}
template <>
void Box<::craby::cojsoncorern::bridging::CojsonCoreRn>::allocation::dealloc(::craby::cojsoncorern::bridging::CojsonCoreRn *ptr) noexcept {
  cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$dealloc(ptr);
}
template <>
void Box<::craby::cojsoncorern::bridging::CojsonCoreRn>::drop() noexcept {
  cxxbridge1$box$craby$cojsoncorern$bridging$CojsonCoreRn$drop(this);
}
} // namespace cxxbridge1
} // namespace rust
