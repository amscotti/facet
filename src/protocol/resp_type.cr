module Redis
  enum RespType
    SimpleString
    SimpleError
    Integer
    BulkString
    Array
    Null
    Boolean
    Double
    Map
    Set
    Push
    Attribute
    BigNumber
    BlobError
    VerbatimString
  end
end
