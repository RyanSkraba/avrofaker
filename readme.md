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

[DataFaker]: https://github.com/datafaker-net/datafaker "DataFaker home page"

Specification
==============================================================================

Annotations are added to an Avro schema (also sometimes known as JSON properties).
These are ignored by Avro for serialization and deserialization and schema evolution, but can have any value to help interpret the data.
In this case, we're not interpreting the data, we're describing how to create it.

In general, the `faker` attribute describes the **strategy** to use to generate fake data.
Other arguments can be present alongside this attribute to configure it.

In the example below:

* The `id` (`INT`) field has the annotation `"faker": "sequence"`, so it uses the **sequence** strategy to generate integers starting from zero.
* The `price`  (`DOUBLE`) field selects the **random** strategy to generate random 64-bit floating point numbers, uniformly over the range given by the argument annotations `"min": 10, "max": 20` (i.e. the interval `[10, 20)`).
* The `code` (`STRING`) field has the argument annotation `"expression": "#{examplify 'A999'"` which implicitly selects the **expression** strategy using [DataFaker] expressions to create codes like `Y929`.

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
    "name" : "price",
    "doc" : "Generates a double value in the interval [10, 20)",
    "type" : "double",
    "faker" : "random",
    "min" : 10,
    "max" : 20
  }, {
    "name" : "code",
    "doc" : "Generates a string value in the form Y929",
    "type" : "string",
    "expression" : "#{examplify 'A999'}"
  } ]
}
```

Note:

- If the `faker` strategy is missing for a type, it can be implicitly detected from any of its unique arguments.
  Each type has a list of strategies that are checked in order of priority, and a default strategy to fall back on.
  In the example above, the `STRING` field `name` doesn't have a strategy.
- If the `faker` annotation contains an object, then *that* object is used to detect the strategy, inheriting and overriding the arguments from the parent.
  This is useful for composing strategies.

Generating numeric data: `INT`, `LONG`, `FLOAT` and `DOUBLE` data
------------------------------------------------------------------------------

These generation strategies and their arguments apply to all numeric types:

* **random** with `min` and `max` (default)
* **gauss** with `min`, `max`, **`mean`** and **`stddev`**
* **sequence** with `min`, `max`, **`start`** and **`step`**
* **value** with **`value`**
* **oneof** with **`oneof`** and **`index`**
* **sumof**, **productof**, **minof**, **maxof**, **meanof** are all aggregate strategies that have a single attribute of the same name.
* **weights** with **`weights`**

The arguments in bold can be used to implicitly select the strategy (in this order of priority).

Notes:
- The strategies are explained for the `INT` type but apply to all the numeric types, using their respective bounds and precision in Java. 
- The default `min` and `max` for **random** `FLOAT` and `DOUBLE` are `0.0` and `1.0` (not their minimum and maximum values).
  Likewise, the default `mean` and `stddev` are `0.0` and `1.0`.
- The default `min` and `max` for a floating point **sequence** are their minimum and maximum values.
- Where values are truncated for whole numbers, they are retained with as much precision as possible for floating point values.
- The **weights** always generates non-negative whole numbers with a maximum at most `2147483647` even when applied to floating point types.

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

### The **weights** strategy with `INT`

The **weights** strategy generates `INT` values non-uniformly starting at 0 up to `max`.
This is normally used for small integer values, where certain values appear more frequently than others (i.e. a higher weight increases its likelihood of being chosen.)
It can be combined with an `index` argument (with the **oneof** strategy, for example) to make non-uniform choices while generating fake values.

This strategy has the following arguments (bolded arguments auto-select the strategy if no other has been explicitly selected):

- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483647 for `INT`).
- **`weights`** An array containing multipliers that affects how often a value (the index of the weight in the array) is chosen relative to the others.
  Putting a `2` in an array otherwise containing only `1` causes the index of the `2` to be chosen twice as frequently as the others.
  (default: `[]` for a uniform distribution).

| Schema                                                                                 | Summary                                                                                                                 |
|----------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "faker": "weights"}`                                                  | Generates non-negative integers, uniformly.                                                                             |
| `{"type": "int", "faker": "weights", "weights": []}`                                   | :arrow_up: Equivalent, no weights are assigned.                                                                         |
| `{"type": "int", "faker": "weights", "weights": [1]}`                                  | :arrow_up: Equivalent. Zero is given a weight of `1`, which is the same as numbers not in the array.                    |
| `{"type": "int", "weights": [1, 1]}`                                                   | :arrow_up: Still equivalent, zero and 1 are given a weight of `1`, like all the other numbers.                          |
| `{"type": "int", "weights": [2, 1], "max": 2}`                                         | Twice the chance of picking `0` rather than `1`.                                                                        |
| `{"type": "int", "max": 10, "weights": [0, 0, 1, 1, 0, 1, 0, 1, 0, 0]}`                | Only choose prime numbers between 0 and 10.                                                                             |
| `{"type": "int", "max": 10, "weights": [1, 2]}`                                        | Twice the chance of picking `1` out of the single digit numbers.                                                        |
| `{"type": "int", "max": 10, "weights": [1, 1, 1, 9]}`                                  | 50% chance of picking `3` out of the single digit numbers.                                                              |
| `{"type": "int", "max": 10, "weights": [7, 3.5, 3.5]}`                                 | One third chance of picking `0`, a third picking `1` or `2` and one third picking the rest of the single digit numbers. |
| `{"type": "int", "faker": [999, {"min": 0, "max": 10}], "index": {"weights": [7, 3]}}` | Picks `999` 70% of the time, and a single digit number the other 30%.                                                   |

Generating `STRING` data
------------------------------------------------------------------------------

These generation strategies and their arguments apply to the `STRING` type:

* **random** with **`length`**, `min` and `max` to specify the number of characters in the string (default)
* **value** with **`value`** to specifies the string to return.
* **oneof** with **`oneof`** and **`index`** selects a strategy to generate the string. 
* **sumof** with **`sumof`** performs string concatenation.
* **expression** with **`expression`** to generate data using the [DataFaker] library.

The arguments in bold can be used to implicitly select the strategy (in this order of priority).

### The **random** strategy with `STRING`

The default strategy (without any annotation, or by setting `faker` to `random`) generates a random 10 character alphanumeric string.

This strategy has the following argument:

- `length` to specify the lower bounds (inclusive) of the generated value (default: `10`).
- `min` to specify the lower bounds (inclusive) of the generated value (default: `0`, constrainted to non-negative).
- `max` to specify the upper bounds (exclusive) of the generated value (default: **TODO - should have a max?**).

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
| `{"type": "string", "min": 5, "max": 10, "length": {}}`          | :arrow_up: Equivalent, because both arguments are taken from the parent.                          |
| `{"type": "string", "min": 5, "max": 10}`                        | Uniformly generates 10 character alphanumeric strings (the default `size` is used).               |

### The **value**, **oneof** and **sumof** strategy with `STRING`

These two strategies both apply to `STRING` data as in `INT` data 
- with **value**, you can specify a values or a set of values to choose from randomly, and 
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

The AvroFaker library wraps [DataFaker] and uses it to generate strings with the **expression** strategy.

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

An `ARRAY` is a composite type containing a collection of Avro data (all of the same type, found in the `items` attribute in the schema, also known as the element type).

By default, it generates a list of three to five items.
The `faker` annotations on the item type are used to generate each item.

This strategy has the following arguments (which auto-select the strategy if no other strategy has been explicitly selected):

- `length` allows you to create an `INT` that specifies the length of the `ARRAY` to create (i.e. the number of items it contains).

| Schema                                                                                                                                  | Summary                                                                                                                                                  |
|-----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "array", "items": "int"}`                                                                                                     | Generates random integer arrays: `[-845367200, -965429156, 604591031]`, `[-1316484263, -1333195387, 377907320, 1350546348, 1514351261]`...               |
| `{"type": "array", "items": "int", "length": {"min": 3, "max": 6}}`                                                                     | :arrow_up: Equivalent, but explicitly sets the length of each array.                                                                                     |
| `{"type": "array", "items": "int", "length": 2}`                                                                                        | Generates random integer pairs: `[-1630935619, -1483802595]`, `[-864264928, -530909147]`...                                                              |
| `{"type": "array", "items": {"type": "int", "step": 1}, "length": 2}`                                                                   | Generates a sequence of integer pairs: `[0,1]`, `[2,3]`, `[4,5]`...                                                                                      |
| `{"type": "array", "items": {"type": "int", "index": {"step": 1}, "oneof": [{"min": 100, "max": 110}, {"step": 1}, 999]}, "length": 3}` | Generates a sequence of integer triples, each using a different generation strategy: `[100,0,999]`, `[109,1,999]`, `[109,2,999]`...                      |
| `{"type": "array", "items": "string", "length" : 2}`                                                                                    | Generates pairs of two letter Strings: `["CC", "zL"]`), `["NH", "BF"]`, `["Hu", "Rv"]`, `["bI", "1i"]`...  Note that the `length` argument is inherited! |
| `{"type": "array", "items": {"type": "string", "length": 1}, "length" : 3}`                                                             | Generates triples of one letter Strings: `["A", "B", "B"]`, `["A", "A", "B"]`...                                                                         |
| `{"type": "array", "min": 0, "max": 10, "items": "double"}`                                                                             | Generates arrays of doubles, 0-9 numbers in an array, each number between [0,10).                                                                        |

- TODO: **value** strategy creating a default value
- TODO: resetting the sequence?

Generating `MAP` data
------------------------------------------------------------------------------

A `MAP`, like an `ARRAY` is a composite type containing a collection of `STRING` keys and Avro data values (all values the same type, found in the `values` attribute in the schema).

By default, it generates a map with three to five items key/value pairs, where each key is String generated using the default **random** string strategy.
The keys are always `STRING`, and the `faker` annotations on the value type are used to generate each value.

This strategy has the following arguments (which auto-select the strategy if no other strategy has been explicitly selected):

- `length` allows you to create an `INT` that specifies the length of the `MAP` to create (i.e. the number of key/value pairs it contains).
- `key` allows you to select and configure the strategy for generating the keys.

| Schema                                                                                                                                 | Summary                                                                                                                                                                 |
|----------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "map", "values": "int"}`                                                                                                     | Generates a random map three to five key/value pairs, using 10 character random strings as keys and random integers as values.                                          |
| `{"type": "map", "values": "int", "key": {"length": 10}, "length": {"min": 3, "max": 6}}`                                              | :arrow_up: Equivalent, but explicitly sets the size of each map and the key function.                                                                                   |
| `{"type": "map", "values": "int", "length": 2}`                                                                                        | Each map contains exactly two key/value pairs, and `length` is inherited by the key generator, so the strings are 2 characters: `{"CC": 295101692, "HB": 604591031}`... |
| `{"type": "map", "values": {"type": "int", "index": {"step": 1}, "oneof": [{"min": 100, "max": 110}, {"step": 1}, 999]}, "length": 3}` | Contains exactly three key/value pairs, each generated by a different strategy: `{"CCz": 107, "NHB": 0, "FHu": 999}`...                                                 |
| `{"type": "map", "values": {"type": "int", "step": 1}, "length": 2}`                                                                   | Two key/value pairs and the integer values are an increasing sequence: `{"CC": 0, "zL": 1}`, `{"NH": 2, "BF": 3}`...                                                    |
| `{"type": "map", "values": "string", "length" : 2}`                                                                                    | Two key/value pairs and both are 2 character strings: `{"CC": "zL", "NH": "BF"}`, `{"Hu": "Rv", "bI": "1i"}`...                                                         |
| `{"type": "map", "values": {"type": "string", "length": 1}, "length" : 3}`                                                             | Three key/value pairs, keys are 3 characters and values are 1: `{"CCz": "L", "NHB": "F", "HuR": "v"}`...                                                                |
| `{"type": "map", "min": 0, "max": 10, "values": "double"}`                                                                             | Zero to ten keys, each key has zero to ten characters and each value is a double from 0.0 to 10.0                                                                       |
| `{"type": "map", "values": {"type": "int", "min": 10, "max": 100}, "key": {"expression": "#{Address.country}"}}`                       | Three to five key/value pairs, each key is a country name and each value is a random int between 10 and 100: `{"Luxembourg": 57, "France": 45, "Moldova": 93}`          |

Generating `BYTES` data
------------------------------------------------------------------------------

A `BYTES` data contains any binary data.

By default, it generates a sequence of uniformly distributed bytes, between 16 to 32 bytes.

This strategy has the following argument:

- `length` allows you to create an `INT` that specifies the length of the `BYTES` to create (i.e. the number of bytes it contains).

| Schema                                                | Summary                                                     |
|-------------------------------------------------------|-------------------------------------------------------------|
| `"bytes"`                                             | Generates a 16-32 byte sequence.                            |
| `{"type": "bytes"}`                                   | :arrow_up: Avro equivalent.                                 |
| `{"type": "bytes", "length": {"min": 16, "max": 32}}` | :arrow_up: Equivalent but explicitly sets the length        |
| `{"type": "bytes", "length": 8}`                      | Always generates 8 bytes                                    |
| `{"type": "bytes", "length": {"step": 1, "max": 32}}` | Generates zero bytes, one byte, two bytes, three byte, etc. |

- TODO: a strategy generates BYTES from a UTF-8 string
- TODO: a strategy generates BYTES from a base64 string

Generating `FIXED` data
------------------------------------------------------------------------------

A `FIXED` data contains binary data of a specified size

By default, it generates a sequence of uniformly distributed bytes of the required size.

| Schema                                       | Summary                  |
|----------------------------------------------|--------------------------|
| `{"type": "fixed", "name": "f4", "size": 4}` | Always generates 4 bytes |

Generating `ENUM` data
------------------------------------------------------------------------------

An `ENUM` data contains a string that corresponds to one of the enumeration symbols in the schema.

This works exactly as the `STRING` **oneof** strategy using the possible symbol names as the values to pick from.

This strategy has the following argument:

- `index` to specify which index to use to pick the strategy (default: Uniformly distributed among the possible indices)
  If the `index` is a generator, it has an implicit `min` and `max` corresponding to the size of the array.

| Schema                                                                              | Summary                           |
|-------------------------------------------------------------------------------------|-----------------------------------|
| `{"type": "enum", "name": "abc", "symbols": ["A", "B", "C"]}`                       | Randomly picks one of the symbols |
| `{"type": "enum", "name": "abc", "symbols": ["A", "B", "C"], "index": {"step": 1}}` | Cycles through the symbols        |

Generating `UNION` data
------------------------------------------------------------------------------

A `UNION` is a composite type containing a single Avro data that can be any one of a collection of types.
The `faker` annotations on the union types are used to generate each item.

A union can't have any properties to configure itself.
To get around this, its arguments are placed in the first union type under the `union` property.

This strategy has the following argument (which auto-select the strategy if no other strategy has been explicitly selected):

This strategy has the following argument:

- `index` to specify which index to use to pick the type to generate (default: Uniformly distributed among the possible types)
  If the `index` is a generator, it has an implicit `min` and `max` corresponding to the number of types available.

| Schema                                                                             | Summary                                                     |
|------------------------------------------------------------------------------------|-------------------------------------------------------------|
| `["int", "null"]`                                                                  | Generates **random** ints and `null` about 50% of the time. |
| `[{"type", "int"}, "null"]`                                                        | :arrow_up: Avro equivalent.                                 |
| `[{"type": "string", "union": {"index": {"step": 1}}, {"type": "int", "step": 2}]` | Alternates between a sequence of strings and even numbers.  |

Generating `BOOLEAN` data
------------------------------------------------------------------------------

The `BOOLEAN` type can contain either true or false.

All the strategies that can be used to generate an `INT` can be used to generate a boolean, which acts like a whole number with only the value values of zero and one.
Zero corresponds to false.

| Schema                           | Summary                                                |
|----------------------------------|--------------------------------------------------------|
| `"boolean"`                      | Generates a random boolean value, equally distributed. |
| `{"type", "boolean"}`            | :arrow_up: Avro equivalent.                            |
| `{"type", "boolean", "step": 1}` | Alternates true and false, starting on false.          |

Generating `NULL` data
------------------------------------------------------------------------------

There is no strategy to generate `NULL` data: it's always `null`.

| Schema             | Summary                     |
|--------------------|-----------------------------|
| `"null"`           | Generates `null`.           |
| `{"type", "null"}` | :arrow_up: Avro equivalent. |

Generating `RECORD` data
------------------------------------------------------------------------------

A `RECORD` is a composite type containing a collection of Avro data called fields.
Each field has a name and a type, and the annotated type is used to generate data for the field.
Properties on the field are also applied to the strategy.

| Schema                                                                                                                                                        | Summary                                                               |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `{"type": "record", "name": "A", "fields": [{"name": "a1", "type": "int", "step": 1}, {"name": "a2", "type": "string", "expression": "#{Name.first_name}"}]}` | Generate a two field record with an incrementing ID and a first name. |

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
