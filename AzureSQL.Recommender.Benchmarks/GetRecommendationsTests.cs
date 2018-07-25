using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace AzureSQL.Recommender.Benchmarks
{
    [TestClass]
    public class GetRecommendationsTests
    {
        private static readonly string _connectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static readonly string _queriesLocation = ConfigurationManager.AppSettings["QueriesLocation"];
        private static Dictionary<string, Dictionary<string, Dictionary<int, object>>> _testData;

        private static string[] ReadSQLQueryPersonData(string sampleName, string sqlQueryName, int personId)
        {
            return File.ReadAllLines(Path.Combine(_queriesLocation, "Results", $"{sqlQueryName} {sampleName} {personId}.txt"));
        }

        private static Dictionary<int, object> Parse<T>(string sampleName, string sqlQueryName, List<int> peopleIds, Func<string, T> func, Comparison<T> comparison)
        {
            var result = new Dictionary<int, object>();
            foreach (var personId in peopleIds)
            {
                var data = ReadSQLQueryPersonData(sampleName, sqlQueryName, personId);
                var dataT = data.Select(x => func(x)).ToList();
                dataT.Sort(comparison);
                result.Add(personId, dataT);
            }

            return result;
        }

        private static readonly Comparison<int> comparison1 = (x, y) => x.CompareTo(y);
        private static readonly Comparison<(int, int)> comparison2 = (x, y) =>
        {
            if (x.Item1 == y.Item1)
            {
                return x.Item2.CompareTo(y.Item2);
            }
            return x.Item1.CompareTo(y.Item1);
        };

        [ClassInitialize]
        public static void Init(TestContext testContext)
        {
            // string SampleName, string QueryName, int PersonId, object Result
            _testData = new Dictionary<string, Dictionary<string, Dictionary<int, object>>>()
            {
                {
                    "S10000",
                    new Dictionary<string, Dictionary<int, object>>()
                    {
                        {
                            "SQLQuery1",
                            Parse("S10000", "SQLQuery1", new List<int>() { 1002784, 1008551, 1008556 }, s =>
                            {
                                return int.Parse(s);
                            }, comparison1)
                        },
                        {
                            "SQLQuery2",
                            Parse("S10000", "SQLQuery2", new List<int>() { 1002784, 1008551, 1008556 }, s =>
                            {
                                var parts = s.Split(' ');
                                return (int.Parse(parts[0]), int.Parse(parts[1]));
                            }, comparison2)
                        },
                        {
                            "SQLQuery3",
                            Parse("S10000", "SQLQuery3", new List<int>() { 1000067, 1000215, 1002802, 1008951 }, s =>
                            {
                                return int.Parse(s);
                            }, comparison1)
                        },
                        {
                            "SQLQuery4",
                            Parse("S10000", "SQLQuery4", new List<int>() { 1002442 }, s =>
                            {
                                return int.Parse(s);
                            }, comparison1)
                        }
                    }
                }
            };
        }

        [DataTestMethod]
        [DataRow("S10000", 1002784, false)]
        [DataRow("S10000", 1008551, false)]
        [DataRow("S10000", 1008556, false)]
        public void TestQuery1(string sampleName, int personId, bool checkAnswer)
        {
            var sqlQueryName = "SQLQuery1";

            var query = string.Format(
                format: File.ReadAllText(Path.Combine(_queriesLocation, $"{sqlQueryName}.sql")),
                arg0: personId);

            using (var db = new SqlConnection(_connectionString))
            {
                var actual = db.Query<int>(query).ToList();

                if (checkAnswer)
                {
                    var expected = _testData[sampleName][sqlQueryName][personId] as List<int>;

                    Assert.AreEqual(expected.Count, actual.Count);

                    actual.Sort(comparison1);

                    for (int i = 0; i < expected.Count; ++i)
                    {
                        if (actual[i] != expected[i])
                        {
                            Assert.Fail();
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow("S10000", 1002784)]
        [DataRow("S10000", 1008551)]
        [DataRow("S10000", 1008556)]
        public void TestQuery1CheckAnswer(string sampleName, int personId)
        {
            TestQuery1(sampleName, personId, true);
        }

        [DataTestMethod]
        [DataRow("S10000", 1002784, false)]
        [DataRow("S10000", 1008551, false)]
        [DataRow("S10000", 1008556, false)]
        public void TestQuery2(string sampleName, int personId, bool checkAnswer)
        {
            var sqlQueryName = "SQLQuery2";

            var query = string.Format(
                format: File.ReadAllText(Path.Combine(_queriesLocation, $"{sqlQueryName}.sql")),
                arg0: personId);

            using (var db = new SqlConnection(_connectionString))
            {
                var actual = db.Query<(int, int)>(query).ToList();

                if (checkAnswer)
                {
                    var expected = _testData[sampleName][sqlQueryName][personId] as List<(int, int)>;

                    Assert.AreEqual(expected.Count, actual.Count);

                    actual.Sort(comparison2);

                    for (int i = 0; i < expected.Count; ++i)
                    {
                        if (actual[i].Item1 != expected[i].Item1 || actual[i].Item2 != expected[i].Item2)
                        {
                            Assert.Fail();
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow("S10000", 1002784)]
        [DataRow("S10000", 1008551)]
        [DataRow("S10000", 1008556)]
        public void TestQuery2CheckAnswer(string sampleName, int personId)
        {
            TestQuery2(sampleName, personId, true);
        }


        [DataTestMethod]
        [DataRow("S10000", 1000067, false)]
        [DataRow("S10000", 1000215, false)]
        [DataRow("S10000", 1002802, false)]
        [DataRow("S10000", 1008951, false)]
        public void TestQuery3(string sampleName, int personId, bool checkAnswer)
        {
            var sqlQueryName = "SQLQuery3";

            var query = string.Format(
                format: File.ReadAllText(Path.Combine(_queriesLocation, $"{sqlQueryName}.sql")),
                arg0: personId,
                arg1: 4);

            using (var db = new SqlConnection(_connectionString))
            {
                var actual = db.Query<int>(query).ToList();

                if (checkAnswer)
                {
                    var expected = _testData[sampleName][sqlQueryName][personId] as List<int>;

                    Assert.AreEqual(expected.Count, actual.Count);

                    actual.Sort(comparison1);

                    for (int i = 0; i < expected.Count; ++i)
                    {
                        if (actual[i] != expected[i])
                        {
                            Assert.Fail();
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow("S10000", 1000067)]
        [DataRow("S10000", 1000215)]
        [DataRow("S10000", 1002802)]
        [DataRow("S10000", 1008951)]
        public void TestQuery3CheckAnswer(string sampleName, int personId)
        {
            TestQuery3(sampleName, personId, true);
        }

        [DataTestMethod]
        [DataRow("S10000", 1002442, false)]
        public void TestQuery4(string sampleName, int personId, bool checkAnswer)
        {
            var sqlQueryName = "SQLQuery4";

            var query = string.Format(
                format: File.ReadAllText(Path.Combine(_queriesLocation, $"{sqlQueryName}.sql")),
                arg0: personId,
                arg1: 1);

            using (var db = new SqlConnection(_connectionString))
            {
                var actual = db.Query<int>(query).ToList();

                if (checkAnswer)
                {
                    var expected = _testData[sampleName][sqlQueryName][personId] as List<int>;

                    Assert.AreEqual(expected.Count, actual.Count);

                    actual.Sort(comparison1);

                    for (int i = 0; i < expected.Count; ++i)
                    {
                        if (actual[i] != expected[i])
                        {
                            Assert.Fail();
                        }
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow("S10000", 1002442)]
        public void TestQuery4CheckAnswer(string sampleName, int personId)
        {
            TestQuery4(sampleName, personId, true);
        }
    }
}
