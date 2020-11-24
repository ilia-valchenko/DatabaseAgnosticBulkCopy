using System;

namespace FunWithSqlBulkCopy
{
    public sealed class Person
    {
        public Guid Id { get; set; }

        public int Number { get; set; }

        public string Firstname { get; set; }

        public string Lastname { get; set; }

        public string City { get; set; }

        public string Street { get; set; }

        public int SettingsId { get; set; }

        public int Age { get; set; }
    }
}