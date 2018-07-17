using System;
using System.Collections.Generic;

namespace AzureSQL.Recommender.Import
{
    public static class Extensions
    {
        public static IEnumerable<IList<T>> ByChunks<T>(this IList<T> list, int chunkSize)
        {
            if (chunkSize < 1)
            {
                throw new ArgumentException("Chunk size can not be less than 1.");
            }

            int chunksCount = list.Count / chunkSize;

            int lastChunksSize = list.Count % chunkSize;
            if (lastChunksSize == 0)
            {
                lastChunksSize = chunkSize;
            }
            else
            {
                ++chunksCount;
            }

            for (int chunkNumber = 0; chunkNumber < chunksCount; ++chunkNumber)
            {
                int offset = chunkNumber * chunkSize;
                int currentChunkSize = chunkSize;
                if (lastChunksSize > 0 && chunkNumber == chunksCount - 1)
                {
                    currentChunkSize = lastChunksSize;
                }

                var result = new List<T>(currentChunkSize);
                for (int i = offset; i < offset + currentChunkSize; ++i)
                {
                    result.Add(list[i]);
                }

                yield return result;
            }
        }
    }
}
