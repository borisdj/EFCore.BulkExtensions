using System;

namespace EFCore.BulkExtensions;

static class SpanSplitExtensions
{
    public static TokenSplitEnumerator<T> Split<T>(this ReadOnlySpan<T> span, ReadOnlySpan<T> delimiters)
        where T : IEquatable<T>
    {
        return new TokenSplitEnumerator<T>(span, delimiters);
    }

    public ref struct TokenSplitEnumerator<T> where T : IEquatable<T>
    {
        readonly ReadOnlySpan<T> _delimiters;
        ReadOnlySpan<T> _span;

        public TokenSplitEntry<T> Current { get; private set; }

        public TokenSplitEnumerator(ReadOnlySpan<T> span, ReadOnlySpan<T> delimiters)
        {
            this._span = span;
            this._delimiters = delimiters;
            this.Current = default;
        }

        public TokenSplitEnumerator<T> GetEnumerator() => this;

        public bool MoveNext()
        {
            var span = this._span;

            if (span.Length == 0)
            {
                return false;
            }

            var index = span.IndexOfAny(this._delimiters);
            if (index == -1)
            {
                this._span = ReadOnlySpan<T>.Empty;
                this.Current = new TokenSplitEntry<T>(span, ReadOnlySpan<T>.Empty);
                return true;
            }

            this.Current = new TokenSplitEntry<T>(span[..index], span.Slice(index, 1));
            this._span = span[(index + 1)..];

            return true;
        }
    }

    public readonly ref struct TokenSplitEntry<T>
    {
        public ReadOnlySpan<T> Token { get; }
        public ReadOnlySpan<T> Delimiters { get; }
        
        public TokenSplitEntry(ReadOnlySpan<T> token, ReadOnlySpan<T> delimiters)
        {
            this.Token = token;
            this.Delimiters = delimiters;
        }

        public void Deconstruct(out ReadOnlySpan<T> token, out ReadOnlySpan<T> delimiters)
        {
            token = this.Token;
            delimiters = this.Delimiters;
        }

        public static implicit operator ReadOnlySpan<T>(TokenSplitEntry<T> entry) => entry.Token;
    }
}
