using System;
using System.Collections.Generic;

namespace FunWithSqlBulkCopy
{
    class Program
    {
        private const string TableName = "Person";
        private const string MSSQLConnectionString =
            "Server=localhost;Database={msSqlDatabaseName};User Id={user};Password={password};";
        private const string PostgreSQLConnectionString =
            "User ID=postgres;Password={password};Host=localhost;Port=5432;Database={postgreSqlDatabaseName};";

        static void Main()
        {
            var helper = new Helper(PostgreSQLConnectionString);

            var newPersons = new List<Person>
            {
                new Person
                {
                    Id = Guid.NewGuid(),
                    Firstname = "Jonah",
                    Lastname = "Stevenson",
                    Street = "Lytton Trees",
                    Age = 25
                },
                new Person
                {
                    Id = Guid.NewGuid(),
                    Firstname = "Remi",
                    Lastname = "Bourne",
                    Street = "Cliveden Green",
                    Age = 30
                },
                new Person
                {
                    Id = Guid.NewGuid(),
                    Firstname = "Alysha",
                    Lastname = "Dupont",
                    Street = "Buttermere Village",
                    Age = 32
                }
            };

            var mapping = new Dictionary<string, Type>
            {
                { nameof(Person.Id), typeof(Guid) },
                { nameof(Person.Number), typeof(int) },
                { nameof(Person.Firstname), typeof(string) },
                { nameof(Person.Lastname), typeof(string) },
                { nameof(Person.City), typeof(string) },
                { nameof(Person.Street), typeof(string) },
                { nameof(Person.SettingsId), typeof(int) },
                { nameof(Person.Age), typeof(int) }
            };

            helper.BulkCopy<Person>(TableName, newPersons, mapping);

            Console.WriteLine("\n\nTap to continue...");
            Console.ReadKey();
        }
    }
}