using Dapper;
using Microsoft.Data.SqlClient;

namespace DateTime.Application.Models
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
        public Boolean UseDefault { get; set; }

        public static async Task FillValues(SqlConnection connection, List<GlobalParameter> names, CancellationToken token = default)
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

            foreach ( var parameter in results ) { 
                if (parameter.TypeValue[0] == 2) //boolean
                {
                    names.First(x => x.Name.Contains(parameter.Name)).ValueDouble = (double)parameter.LogicValue[0];
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
