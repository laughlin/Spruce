using System.Data;
using NUnit.Framework;
using Spruce.Tests.Infrastructure;
using StructureMap;

namespace Spruce.Tests
{
	public abstract class TestBase
	{
		protected IDbConnection Db { get; set; }

		[TestFixtureSetUp]
		public virtual void SetupFixture()
		{
            var container = new Container(new IocRegistry());
            Db = container.GetInstance<IDbConnection>();
		}
		[TestFixtureTearDown]
		public virtual void TearDownFixture()
		{
		}

		[SetUp]
		public virtual void Setup()
		{
		}

		[TearDown]
		public virtual void TearDown()
		{
		}
	}
}