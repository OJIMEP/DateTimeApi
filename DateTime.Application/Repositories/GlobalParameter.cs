using Dapper;
using Microsoft.Data.SqlClient;

namespace DateTimeService.Application.Repositories
{
    public static class GlobalParamListExtensions
    {
        public static double GetValue(this List<GlobalParameter> list, string name)
        {
            return list.First(x => x.Name.Contains(name)).ValueDouble;
        }
    }

    public class GlobalParameter
    {
        public required string Name { get; init; }
        public double ValueDouble { get; set; }
        public double DefaultDouble { get; set; }
        public bool UseDefault { get; set; }

        public static async Task<List<GlobalParameter>> GetParameters(SqlConnection connection, CancellationToken token = default)
        {
            var parameters = new List<GlobalParameter>
            {
                new GlobalParameter
                {
                    Name = "rsp_КоличествоДнейЗаполненияГрафика",
                    DefaultDouble = 5
                },
                new GlobalParameter
                {
                    Name = "КоличествоДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                    DefaultDouble = 4
                },
                new GlobalParameter
                {
                    Name = "ПроцентДнейАнализаЛучшейЦеныПриОтсрочкеЗаказа",
                    DefaultDouble = 3
                },
                new GlobalParameter
                {
                    Name = "Логистика_ЭтажПоУмолчанию",
                    DefaultDouble = 4,
                    UseDefault = true
                },
                new GlobalParameter
                {
                    Name = "ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров",
                    DefaultDouble = 0
                },
                new GlobalParameter
                {
                    Name = "КоличествоДнейСмещенияДоступностиПрослеживаемыхМаркируемыхТоваров",
                    DefaultDouble = 0
                },
                new GlobalParameter
                {
                    Name = "ПриоритизироватьСток_64854",
                    DefaultDouble = 0
                }
            };

            await FillValues(connection, parameters, token);

            return parameters;
        }

        private static async Task FillValues(SqlConnection connection, List<GlobalParameter> names, CancellationToken token)
        {
            string query = """
                SELECT [_Fld22354] As Name,
                    [_Fld22355_TYPE] As TypeValue,     
                    [_Fld22355_L] As LogicValue,    
                    [_Fld22355_N] As NumberValue     
                FROM [dbo].[_InfoRg22353]
                WHERE [_Fld22354] IN({0})
                """;

            var parameters = new DynamicParameters();
            var parameterNames = new string[names.Count];

            for (int i = 0; i < names.Count; i++)
            {
                parameterNames[i] = $"@Article{i}";
                parameters.Add(parameterNames[i], names[i].Name);
            }

            query = string.Format(query, string.Join(", ", parameterNames));

            var results = await connection.QueryAsync<GlobalParametersQueryResult>(
                new CommandDefinition(query, parameters, cancellationToken: token)
            );

            foreach (var parameter in results)
            {
                if (parameter.TypeValue[0] == 2) //boolean
                {
                    names.First(x => x.Name.Contains(parameter.Name)).ValueDouble = parameter.LogicValue[0];
                }
                else
                {
                    names.First(x => x.Name.Contains(parameter.Name)).ValueDouble = (double)parameter.NumberValue;
                }
            }

            foreach (var param in names)
            {
                if (param.UseDefault && param.ValueDouble == 0)
                {
                    param.ValueDouble = param.DefaultDouble;
                }
            }
        }

        private class GlobalParametersQueryResult
        {
            public required string Name { get; set; }
            public byte[] TypeValue { get; set; }
            public byte[] LogicValue { get; set; }
            public decimal NumberValue { get; set; }
        }
    }
}
