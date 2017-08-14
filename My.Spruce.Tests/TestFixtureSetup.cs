using System.Data;
using My.Spruce.Schema;
using My.Spruce.Tests.Infrastructure;
using NUnit.Framework;
using StackExchange.Profiling;
using StructureMap;

namespace My.Spruce.Tests
{
	[SetUpFixture]
	public class TestFixtureSetup
	{
		[SetUp]
		public void Setup()
		{
			MiniProfiler.Settings.ProfilerProvider = new SingletonProfilerProvider();
			MiniProfiler.Start();
			ObjectFactory.Initialize(x => x.AddRegistry(new IocRegistry()));

			// Start with a fresh, empty db
			var db = ObjectFactory.GetInstance<IDbConnection>();
			db.DropAllScriptedObjects();
			db.DropAllTables();
		}
	}
}
