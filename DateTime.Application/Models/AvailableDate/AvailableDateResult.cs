namespace DateTimeService.Application.Models
{
    public class AvailableDateResult
    {
        public bool WithQuantity { get; set; }

        public Dictionary<string, AvailableDateElementResult> Data { get; set; }

        public AvailableDateResult()
        {
            Data = new Dictionary<string, AvailableDateElementResult>();
        }

        public void FillFromAvailableDateRecords(List<AvailableDateRecord> records, AvailableDateQuery query)
        {
            try
            {
                foreach (var codeItem in query.Codes)
                {
                    var resultElement = new AvailableDateElementResult()
                    {
                        Code = codeItem.Article,
                        SalesCode = codeItem.SalesCode
                    };

                    AvailableDateRecord? dbRecord;

                    if (String.IsNullOrEmpty(codeItem.Code))
                    {
                        dbRecord = records.FirstOrDefault(x => x.Article == codeItem.Article);
                    }
                    else
                    {
                        dbRecord = records.FirstOrDefault(x => x.Code == codeItem.Code);
                    }

                    if (dbRecord is not null)
                    {
                        resultElement.Courier = dbRecord.Courier.Year != 3999
                            ? dbRecord.Courier.ToString("yyyy-MM-ddTHH:mm:ss")
                            : null;
                        resultElement.Self = dbRecord.Self.Year != 3999
                            ? dbRecord.Self.ToString("yyyy-MM-ddTHH:mm:ss")
                            : null;

                        resultElement.YourTimeInterval = dbRecord.YourTimeInterval;
                    }

                    if (String.IsNullOrEmpty(codeItem.Code))
                    {
                        Data.TryAdd(codeItem.Article, resultElement);
                    }
                    else
                    {
                        Data.TryAdd($"{codeItem.Article}_{codeItem.SalesCode}", resultElement);
                    }
                }
            }
            catch (ArgumentException)
            {
                throw new Exception("Duplicated keys in dictionary");
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
