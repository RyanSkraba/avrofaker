AvroFaker
==============================================================================

[![Java CI with Maven](https://github.com/RyanSkraba/avrofaker/actions/workflows/maven.yml/badge.svg)](https://github.com/RyanSkraba/avrofaker/actions/workflows/maven.yml)

AvroFaker creates generic Avro data from an annotated Avro schema.

As a basic example, to generate random integers between 100 and 200:

```scala
val schema = """{"type": "int", "min": 100, "max": 200}"""
val faker = com.skraba.avrofaker.AvroFaker(new org.apache.avro.Schema.Parser().parse(schema))

// prints 194 134 191 104 181 117 148 133 122 174 130
for (_ <- 0 to 10) print(s"${faker()} ")
```

Every schema and subschema can be annotated, so you can create awesome, customizable fake data for testing!

Specification
==============================================================================

Annotations are added to an Avro schema (also sometimes known as JSON properties).
These are ignored by Avro for serialization and deserialization and schema evolution, but can have any value to help interpret the data.
In this case, we're not interpreting the data, we're describing how to create it.

In general, the `faker` attribute describes the strategy to use to generate fake data.
Other arguments can be present alongside thiis attribute to configure it.

As an example:

```json
{
  "name" : "com.skraba.avrofaker.example.FakeRecord",
  "type" : "record",
  "fields" : [ {
    "name" : "id",
    "doc" : "Generates an integer sequence starting from zero",
    "type" : "int",
    "faker" : "sequence"
  }, {
    "name" : "weight",
    "doc" : "Generates a double value in the interval [10, 20)",
    "type" : "double",
    "faker" : "random",
    "min" : 10,
    "max" : 20
  } ]
}
```

Some notes:

- If the `faker` strategy is missing for a type, it can be implicitly detected from any of its unique arguments.
  Each type has a list of strategies that are checked in order of priority, and a default strategy to fall back on.
- If the `faker` annotation contains an object, then *that* object is used to detect the strategy, inheriting but overriding the arguments from the parent.
  This is useful for composing strategies.

Generating `INT` data
------------------------------------------------------------------------------

The strategies for `INT` data and their arguments include (bold attribute can be used to implicitly select the strategy):

* **random** with `min` and `max` (default)
* **gauss** with `min`, `max`, **`mean`** and **`stddev`**
* **sequence** with `min`, `max`, **`start`** and **`step`**

### The **random** strategy with `INT`

The default strategy (without any annotation, or by setting `faker` to `random`) generates a random Int whole number uniformly distributed among all possible values.

This strategy has the following arguments:

- `min` to specify the lower bounds (inclusive) of the generated value (default: `-2147483648`).
- `max` to specify the upper bounds (exclusive) of the generated value (default: `2147483647`).
  Note that this means that `2147483647` will not be generated.

| Schema                                             | Summary                                                                                          |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `"int"`                                            | Uniformly generates a random 32 bit whole number from `-2147483648` to `2147483646`.             |
| `{"type": "int"}`                                  | :arrow_up: Avro equivalent.                                                                      |
| `{"type": "int", "faker": "random"}`               | :arrow_up: Equivalent.                                                                           |
| `{"type": "int", "faker": {}}`                     | :arrow_up: Equivalent, but useless                                                               |
| `{"type": "int", "faker": {"faker": "random"}}`    | :arrow_up: Equivalent, but useless.                                                              |
| `{"type": "int", "faker": "random", "min": 0}`     | Uniformly generates a random 32 bit whole number from 0 to `2147483646`.                         |
| `{"type": "int", "min": 0}`                        | :arrow_up: Equivalent.                                                                           |
| `{"type": "int", "faker": {"min": 0}}`             | :arrow_up: Equivalent.                                                                           |
| `{"type": "int", "min": 100, "faker": {"min": 0}}` | :arrow_up: Equivalent, because the `min` argument is ignored from the parent when it's supplied. |
| `{"type": "int", "min": 0, "max": 256}`            | :Uniformly generates a number from `0` to `255`                                                  |

### The **gauss** strategy with `INT`

You can explicitly set the **gauss** strategy by setting the `faker` annotation to `gauss`.
This is useful to have numbers that fit in a bell curve.

This strategy has the following arguments (bolded arguments auto-select the strategy if no other has been explicitly selected):

- `min` to specify the lower bounds (inclusive) of the generated value (default: -2147483648).
- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483647).
- **`mean`** to specify the mean value (default: `0`).
- **`stddev`** to specify the standard deviation (default: `100`).

This distribution is calculated using floating point numbers then truncated, so it might not have strictly mathematically correct behaviour, especially around small ranges.
The `min` and `max` values are applied by discarding results outside the interval, so be careful that they are coherent.
For example, there is 1 chance in a million, to generate a number 5 standard deviations from the mean, so setting a `min` of `500` will sift through about a million random values before returning a valid one.

| Schema                                                                   | Summary                                                                                                                                                         |
|--------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "faker": "gauss"}`                                      | Generates a bell curve of numbers centered around `0`, with 95% of the values between `-200` and `200`.                                                         |
| `{"type": "int", "mean": 0}`                                             | :arrow_up: Equivalent, implicitly choosing the **gauss** strategy.                                                                                              |
| `{"type": "int", "faker": "gauss", "mean": 100, "stddev": 10}`           | Generates numbers centered around `100`, with 95% of the values between `80` and `120`. It's very unlikely but possible that a negative value could occur.      |
| `{"type": "int", "faker": "gauss", "mean": 100, "stddev": 10, "min": 0}` | Generates numbers centered around `100`, with 95% of the values between `80` and `120`, guaranteeing no negative values.                                        |
| `{"type": "int", "min": 0, "faker": {"stddev": 10}}`                     | Generates a bell curve of numbers centered around `0` and standard deviation of `10`, but only accepts positive values (half are thrown away).                  |
| `{"type": "int", "min": 50, "faker": {"mean": 100, "stddev": 10}}`       | :warning: The same bell curve as above, but only numbers > `50`. This is 5 standard deviations from the mean; one one value out of every million will be valid. |

### The **sequence** strategy with `INT`

You can explicitly set the **gauss** strategy by setting the `faker` annotation to `sequence`.
This generates a sequence of whole numbers with an equal step between them.

This strategy has the following arguments (bolded arguments auto-select the strategy if no other has been explicitly selected):

- `min` to specify the lower bounds (inclusive) of the generated value (default: -2147483648).
- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483647).  
- **`step`** to specify the distance between generated sequential numbers (default: 1).
- **`start`** to specify the starting value for the sequence. If this value is outside the intervale [`min`, `max`), then it is ignored and the default is used.
  - (default: `min` if explicitly specified, otherwise `0` for constant or increasing sequences, and `max` for decreasing sequences).

If the `step` is zero or positive, the sequence starts generating at the `min` value and monotonically grows by that value every time.
When it would pass the `max` value, the sequence restarts at the beginning.
If the `step` is greater than one, the remainder (if any) is "wrapped around" past `min`.

If the `step` is negative, the sequence starts generating at the `max` value, etc and monotonically decreases, etc.

A zero `step`, of course, starts and stays at `min` forever.

| Schema                                                       | Summary                                                                                                         |
|--------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "faker": "sequence"}`                       | `0`, `1`, `2`, `3`, `4`, `5` until it reaches `2147483646`, then restarts.                                      |
| `{"type": "int", "step": 1}`                                 | :arrow_up: Equivalent, implicitly choosing the **sequence** strategy.                                           |
| `{"type": "int", "start": 10000}`                            | :arrow_up: `10000`, `10001`, `10002`, `10003`, `10004`, `10005`, implicitly choosing the **sequence** strategy. |
| `{"type": "int", "step": -1, max": 4}`                       | `3`, `2`, `1`, `0`, `-1`, `-2`, etc.                                                                            |
| `{"type": "int", "step": -1, "min": 0, max": 4}`             | `3`, `2`, `1`, `0`, `3`, `2`, etc.                                                                              |
| `{"type": "int", "start": 3, "min": 0, "max": 4, "step": 3}` | :arrow_up: Equivalent because of the wrapped remainder, but confusing.                                          |
| `{"type": "int", "faker": {"start": 10, "max": 13}}`         | `10`, `11`, `12`, `0`, `1`, `2`, etc.                                                                           |
| `{"type": "int", "max": 13, "faker": {"start": 10}}`         | :arrow_up: Equivalent, inheriting the `max` argument from the parent.                                           |

### The **value** strategy with `INT`

You can generate a constant value with the **value** strategy by setting the `faker` attribute to `value`, or by setting the `faker` attribute to a JSON number.

This strategy has the following arguments (bolded arguments auto-select the strategy if no other has been explicitly selected):

- `min` to specify the lower bounds (inclusive) of the generated value (default: 0).
- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483647 for `INT`).
- **`value`** to specify the constant value to return (default: 0).  If this is out of the interval, it's moved into the interval.

| Schema                                             | Summary                                                |
|----------------------------------------------------|--------------------------------------------------------|
| `{"type": "int", "value": 123}`                    | Always generates `123`.                                |
| `{"type": "int", "faker": 123}`                    | :arrow_up: Equivalent explicit shortcut.               |
| `{"type": "int", "faker": "value", "value": 123}`  | :arrow_up: Equivalent, explicit stategy with argument. |
| `{"type": "int", "value": 123.9}`                  | Truncates floating point numbers to generate `123`.    |
| `{"type": "int", "value": "123.1"}`                | And converts strings to generate `123`.                |
| `{"type": "int", "min": "999.9", value": "123.1"}` | Applies bounds to generate `999`.                      |

### The **oneof** strategy for an `INT`

You can explicitly set the **oneof** strategy by setting the `faker` annotation to `oneof`.
This picks a strategy out of an array so you can compose different strategies.
This is also automatically selected with a random `index` if the `faker` annotation is a JSON array.

This strategy has the following arguments (bolded arguments auto-select the strategy if no other has been explicitly selected):

- **`oneof`** to specify the set of possible strategies to use (default: the default generator for this type: a single **random** generator). 
  If the `faker` annotation has a JSON array value, it's used instead of this parameter.
- **`index`** to specify which index to use to pick the strategy (default: Uniformly distributed among the possible indices)
  If the `index` is a generator, it has an implicit `min` and `max` corresponding to the size of the array.

| Schema                                                                                  | Summary                                                                                       |
|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `{"type": "int", "faker": [123, 234, 345]}`                                             | Randomly picks `123`, `234` or `345`.                                                         |
| `{"type": "int", "oneof": [123, 234, 345]}`                                             | :arrow_up: Equivalent, but implicitly selects the strategy.                                   |
| `{"type": "int", "faker": [999, {"min": 0, "max": 9}]}`                                 | Picks `999` 50% of the time, and a single digit number the other 50%.                         |
| `{"type": "int", "oneof": [123, 321, 999], "index": 0}`                                 | Always generates `123`.                                                                       |
| `{"type": "int", "oneof": [123, 321, 999], "index": -1}`                                | :arrow_up: Equivalent, the implicit `min` on the index forces it to `0`.                      |
| `{"type": "int", "oneof": [123, 321, 999], "index": {"step": 1}}`                       | The index is a sequence `0`,`1`,`2`,`0`,`1`, etc, so alternates `123`,`321`,`999`,`123`, etc. |
| `{"type": "int", "oneof": [1,2,3,4,5], "index": {"gauss": {"mean": 2.5, "stddev": 1}}}` | Picks the numbers `1`,`2`,`3`,`4`,`5` in a rough bell curve.                                  |

### Other aggregate strategies like **oneof** strategy for an `INT`

The following strategies work like **oneof** (without the `index` argument).  
They each have an argument that is the same as their strategy name, but applies to all of the values in the JSON array.

- **sumof** uses every element of the array is used to generate an integer and the sum is returned.  
  Be careful about inheriting `min` and `max`, since they apply to *each* generated integer separately.
- **productof** uses every element of the array is used to generate an integer and their product is returned.
- **minof** uses every element of the array is used to generate an integer and the minimum is returned.
- **maxof** uses every element of the array is used to generate an integer and the maximum is returned.
- **meanof** uses every element of the array is used to generate an integer and the average is returned.

| Schema                                                    | Summary                                                    |
|-----------------------------------------------------------|------------------------------------------------------------|
| `{"type": "int", "sumof": [1,2,3,4,5]}`                   | Always generates `15`.                                     |
| `{"type": "int", "sumof": [1,2,3,4,5]}, "faker": "sumof"` | :arrow_up: Equivalent but explicitly selects the strategy. |
| `{"type": "int", "productof": [1,2,3,4,5]}`               | Always generates `120`.                                    |
| `{"type": "int", "minof": [1,2,3,4,5]}`                   | Always generates `1`.                                      |
| `{"type": "int", "maxof": [1,2,3,4,5]}`                   | Always generates `5`.                                      |
| `{"type": "int", "meanof": [1,2,3,4,5]}`                  | Always generates `3`.                                      |

Generating `LONG`, `FLOAT` and `DOUBLE` data
------------------------------------------------------------------------------

The same rules and strategies apply to the other numerical types, using their respective bounds and precision in Java.

Notes:
- The default `min` and `max` for **random** `FLOAT` and `DOUBLE` are `0.0` and `1.0` (not their minimum and maximum values).
  Likewise, the default `mean` and `stddev` are `0.0` and `1.0`. 
- The default `min` and `max` for a floating point **sequence** are their minimum and maximum values.
- Where values are truncated for whole numbers, they are retained with as much precision as possible for floating point values.

Generating `STRING` data
------------------------------------------------------------------------------

### The **random** strategy with `STRING`

The default strategy (without any annotation, or by setting `faker` to `random`) generates a random 10 character alphanumeric string.

This strategy has the following argument:

- `length` to specify the lower bounds (inclusive) of the generated value (default: `10`).
- `min` to specify the lower bounds (inclusive) of the generated value (default: `0`, constrainted to non-negative).
- `max` to specify the lower bounds (inclusive) of the generated value (default: **TODO**).

| Schema                                                           | Summary                                                                                           |
|------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `"string"`                                                       | Uniformly generates 10 character alphanumeric strings.                                            |
| `{"type": "string"}`                                             | :arrow_up: Avro equivalent.                                                                       |
| `{"type": "string", "faker": "random"}`                          | :arrow_up: Equivalent.                                                                            |
| `{"type": "string", "faker": {}}`                                | :arrow_up: Equivalent, but useless                                                                |
| `{"type": "string", "faker": {"faker": "random"}}`               | :arrow_up: Equivalent, but useless.                                                               |
| `{"type": "string", "length": {"min": 5, "max": 10}}`            | Uniformly generates a random string with a size bounded from `5` (inclusive) to `10` (exclusive). |
| `{"type": "string", "min": 5, "length: {"max": 10}}`             | :arrow_up: Equivalent, because the `min` argument is taken from the parent.                       |
| `{"type": "string", "min": 100, "length": {"min": 5, max": 10}}` | :arrow_up: Equivalent, because the `min` argument is ignored from the parent when it's supplied.  |
| `{"type": "string", "max": 5, "max": 10, "length": {}}`          | :arrow_up: Equivalent, because both arguments are taken from the parent.                          |
| `{"type": "string", "max": 5, "max": 10}`                        | Uniformly generates 10 character alphanumeric strings (the default `size` is used).               |

### The **value**, **oneof** and **sumof** strategy with `STRING`

These two strategies both apply to `STRING` data as in `INT` data 
- with **value** you can specify a values or a set of values to choose from randomly, and 
- **oneof** allows you to select from the set using an `INT` generator in the `index` argument, and
- **sumof** concatenates the strings that it generates (the equivalent of `+` for numbers).

| Schema                                                                                                   | Summary                                                                                                 |
|----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `{"type": "string", "faker": "value", "value": "Hello"}`                                                 | Always generates `"Hello"`                                                                              |
| `{"type": "string", "faker": "Hello"}`                                                                   | :arrow_up: Equivalent, but implicitly selects the strategy.                                             |
| `{"type": "string", "value": "Hello"}`                                                                   | :arrow_up: Equivalent                                                                                   |
| `{"type": "string", "faker": ["Hello", "Bonjour"}`                                                       | Randomly picks `"Hello"` or `Bonjour`.                                                                  |
| `{"type": "string", "value": ["Hello", "Bonjour"}`                                                       | :arrow_up: Equivalent                                                                                   |
| `{"type": "string", "oneof": ["Hello", "Bonjour"}`                                                       | :arrow_up: Equivalent                                                                                   |
| `{"type": "string", "faker": "oneof", "oneof": ["Hello", "Bonjour"}`                                     | Equivalent, but explicitly selects the strategy.                                                        |
| `{"type": "string", "faker": ["X", {"min": 0, "max": 9}]}`                                               | Picks `"X"` 50% of the time, and a single digit character string the other 50%.                         |
| `{"type": "string", "oneof": ["A", "B", "C"], "index": 0}`                                               | Always generates `"A"`.                                                                                 |
| `{"type": "string", "oneof": ["A", "B", "C"], "index": -1}`                                              | :arrow_up: Equivalent, the implicit `min` on the index forces it to `0`.                                |
| `{"type": "string", "oneof": ["A", "B", "C"], "index": {"step": 1}}`                                     | The index is a sequence `0`,`1`,`2`,`0`,`1`, etc, so alternates `"A"`, `"B"`, `"C"`, `"A"`, `"B"`, etc. |
| `{"type": "string", "oneof": ["A", "B", "C", "D", "E"], "index": {"gauss": {"mean": 2.5, "stddev": 1}}}` | Picks the strings `"A"`, `"B"`, `"C"`, `"D"`, `"E"` in a rough bell curve.                              |
| `{"type": "string", "sumof": ["A", "B", "C"]`                                                            | Always generates `"ABC"`.                                                                               |
| `{"type": "string", "sumof": [{"faker": "expression"}, ":::", {"faker": "expression"}]}`                 | Generates strings like `"Y929:::P626"`, `"V665:::H777"`, `"B539:::E540"`                                |

### The **expression** strategy with `STRING`

The AvroFaker library wraps [DataFaker](https://github.com/datafaker-net/datafaker) and uses it to generate strings with the **expression** strategy.

This strategy has the following argument (which auto-selects the strategy if no other strategy has been explicitly selected):

- **`expression`** to specify the [DataFaker expression](https://www.datafaker.net/documentation/expressions/) to use in generation (Default: `#{examplify 'A999'}`)

| Schema                                                                                               | Summary                                                     |
|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| `{"type": "string", "faker": "expression"}`                                                          | Generates a capital letter and three digits, like `Y929`    |
| `{"type": "string", "expression": "#{examplify 'A999'}"}`                                            | :arrow_up: Equivalent, but explicitly sets the expression.  |
| `{"type": "string", "faker": "expression", "expression": "#{numerify '####'}"}`                      | Generates four digit numbers using DataFaker                |
| `{"type": "string", "expression": "#{numerify '####'}"}`                                             | :arrow_up: Equivalent, but implicitly selects the strategy. |
| `{"type": "string", "expression": "#{letterify 'key-?????'}"}`                                       | Generates 5 letter random words: `"key-lvxts"`              |
| `{"type": "string", "expression": "#{Name.first_name} #{Name.last_name}"}`                           | Generates names: `"Kit Graham"`                             |
| `{"type": "string", "expression": "#{Address.street_address}\n#{Address.city}, #{Address.country}`"} | Generates a three line address.                             |

### The **cast** strategy with `STRING`

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

You can turn a strategy for another primitive faker type to a `STRING` using the **cast** strategy.

This strategy has the following arguments (which auto-select the strategy if no other strategy has been explicitly selected):

- **`double`** allows you to create a `DOUBLE` generator and turn it into a series of Strings, or.
- **`long`** allows you to create a `LONG` generator and turn it into a series of Strings, or
- **`float`** allows you to create a `FLOAT` generator and turn it into a series of Strings, or
- **`int`** allows you to create a `INT` generator and turn it into a series of Strings (Default: A sequence of 0, 1, 2, 3).
- **`pattern`** allows you to set a printf style pattern for formatting a number into a string.

Only one of the arguments `double`, `long`, `float`, `int` will be taken into account (in that order of priority).

| Schema                                                                   | Summary                                                                        |
|--------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `{"type": "string", "faker": "cast"}`                                    | Generates a sequence of `"0"`, `"1"`, `"2"`, `"3"`                             |
| `{"type": "string", "int": {"step": 1}}`                                 | :arrow_up: Equivalent, but explicitly sets the expression.                     |
| `{"type": "string", "double": {"stddev": 1}, "pattern": "%.2f"}`         | Generates numbers in a gaussian distribution, formatted to two decimal places. |
| `{"type": "string", "long": {}, "pattern": "%X"}`                        | Generates 64 bit hexadecimal numbers in upper case.                            |
| `{"type": "string", "int": {"min": 0, "max": 10000}, "pattern": "%04d"}` | Generates random zero-padded 4 digit numbers.                                  |

Generating `ARRAY` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

An `ARRAY` is a composite type containing a number of items.

By default, it generates an list of three to five items, where each item is generated from the annotated items in the schema. 

This strategy has the following arguments (which auto-select the strategy if no other strategy has been explicitly selected):

- **length** allows you to create an `INT` that specifies the length of the `ARRAY` to create.


| Schema                                                                      | Summary                                                                                                                                                    |
|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "array", "items": "int"}`                                         | Generates random integer arrays: `[-845367200, -965429156, 604591031]`, `[-1316484263, -1333195387, 377907320, 1350546348, 1514351261]`...                 |
| `{"type": "array", "items": "int", "length": {"min": 3, "max": 6}}`         | :arrow_up: Equivalent, but explicitly sets the length of each array.                                                                                       |
| `{"type": "array", "items": "int", "length": 2}`                            | Generates random integer pairs: `[-1630935619, -1483802595]`, `[-864264928, -530909147]`...                                                                |
| `{"type": "array", "items": {"type": "int", "step": 1}, "length": 2}`       | Generates a sequence of integer pairs: `[0,1]`, `[2,3]`, `[4,5]`...                                                                                        |
| `{"type": "array", "items": "string", "length" : 2}`                        | Generates pairs of two letter Strings: `["CC", "zL"]`), `["NH", "BF"]`, `["Hu", "Rv"]`, `["bI", "1i"]`...  Note that the `length` argument is inherited! |
| `{"type": "array", "items": {"type": "string", "length": 1}, "length" : 3}` | Generates triples of one letter Strings: `["A", "B", "B"]`, `["A", "A", "B"]`...                                                                           |
| `{"type": "array", "min": 0, "max": 10, "items": "double"}`                  | Generates arrays of doubles, 0-9 numbers in an array, each number between [0,10).                                                                          |

- TODO: **value** strategy creating a default value
- TODO: resetting the sequence?

Generating `MAP` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Generates an map of 3-5 elements with a random 10 character key and the value (according to it's annotated value type)

Generating `BYTES` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Generates 5 to 10 bytes.

Generating `FIXED` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Random bytes of the right size.

Generating `ENUM` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Randomly picks a symbol.

Generating `UNION` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Generates one of the values, uniformly picking one randomly.

Generating `BOOLEAN` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Generates `true` or `false`, uniformly picking one randomly.

Generating `NULL` data
------------------------------------------------------------------------------

There is no strategy to generate `NULL` data: it's always `null`.

Generating `RECORD` data
------------------------------------------------------------------------------

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.

Generates a GenericRecord with a field for each according to its annotated type.

Using
------------------------------------------------------------------------------

You can import the library into your project from [maven central](https://central.sonatype.com/artifact/com.tinfoiled/docopt4s_2.13):

```xml
<dependency>
  <groupId>com.skraba</groupId>
  <artifactId>avrofaker</artifactId>
  <version>0.0.0</version>
</dependency>
```

Building
------------------------------------------------------------------------------

```sh
# Build, format and run all tests
mvn spotless:apply clean verify
```
