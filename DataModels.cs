using System.Collections.Generic;
using System.Runtime.Serialization;

namespace AzureStorageRestClient
{
    [DataContract(Name = "EnumerationResults", Namespace = "")]
    public class EnumerationResults<T>
    {
        [DataMember(Name = "Entries")]
        public IEnumerable<T> Entries { get; set; }
    }

    [DataContract(Name = "Directory", Namespace = "")]
    public class DirectoryResult
    {
        [DataMember(Name = "Name")]
        public string Name { get; set; }
    }

    [DataContract(Name = "File", Namespace = "")]
    public class FileResult
    {
        [DataMember(Name = "Name")]
        public string Name { get; set; }
    }

}
