namespace Illumina.AntTools.Model
{
    // NOTE: dumb wrapper to work around an issue in which .NET does not release memory allocated for objects removed from a BlockingCollection
    // see: http://stackoverflow.com/questions/12824519/the-net-concurrent-blockingcollection-has-a-memory-leak
    public class HackWrapper<T>
        where T : class
    {
        public T Item { get; set; }

        public HackWrapper(T item) { Item = item; }
    }
}
