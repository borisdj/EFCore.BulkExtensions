#if NET5_0
#else
/// Polyfill so we can use C# 9.0 record types
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}
#endif
