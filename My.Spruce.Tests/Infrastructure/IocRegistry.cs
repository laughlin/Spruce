using System;
using System.Configuration;
using System.Data;
using MySql.Data.MySqlClient;
using StackExchange.Profiling;
using StackExchange.Profiling.Data;
using StructureMap;
using StructureMap.Configuration.DSL;

namespace My.Spruce.Tests.Infrastructure
{
	public class IocRegistry : Registry
	{
		public IocRegistry()
		{
			Scan(scan =>
					{
						scan.AssemblyContainingType<IocRegistry>();
						scan.WithDefaultConventions();
					});

			var connectionString = "Default";
			if (ConfigurationManager.ConnectionStrings[Environment.MachineName] != null)
				connectionString = Environment.MachineName;

            For<MySqlConnection>().Use(() =>
			{
			    return new MySqlConnection(ConfigurationManager.ConnectionStrings[connectionString].ConnectionString);
			});

			For<IDbConnection>()
				.LifecycleIs(new StructureMap.Pipeline.ThreadLocalStorageLifecycle())
				.Use(() =>
					{
                        var connection = ObjectFactory.GetInstance<MySqlConnection>();
						connection.Open();
						return new ProfiledDbConnection(connection, MiniProfiler.Current);						
					})
				.Named("Database Connection");
		}
	}
}
