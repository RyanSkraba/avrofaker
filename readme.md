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

:warning: **DRAFT** :warning: **DRAFT** :warning: **DRAFT** :warning: This spec hasn't been implemented yet, the logic is still being worked out.
==============================================================================

- TODO: Don't use Random.between, use nextLong when there is no `min` or `max` attribute set at all.

Generating `INT` data
------------------------------------------------------------------------------

### Random `INT`s

By default (without any annotation), generates a random Int whole number uniformly distributed among all possible values.

You can explicitly set the **random** strategy by including the `random` annotation on the type.
The value can either be `true` to use the defaults for this strategy, or it can be a JSON object with the following arguments:

- `min` to specify the lower bounds (inclusive) of the generated value (default: `-2147483648`).
- `max` to specify the upper bounds (exclusive) of the generated value (default: `2147483647`).
  Note that this means that `2147483647` will not be generated when you specify a range.

If any strategy argument can be found directly in the (parent) annotated schema, it will be used as the default.

| Schema                                             | Summary                                                                                            |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `int`                                              | Uniformly generates a random 32 bit whole number from `-2147483648` to `2147483647`.               |
| `{"type": "int"}`                                  | :arrow_up: Avro equivalent.                                                                        |
| `{"type": "int", "random": true}`                  | Uniformly generates a random 32 bit whole number from 0 to **`2147483646`** (see bounds).          |
| `{"type": "int", "random": {"min": 0}}`            | :arrow_up: Equivalent, but  **random** generating a  whole number from `0` to **`2147483646`**.    |
| `{"type": "int", "min": 0}`                        | :arrow_up: Equivalent, implicitly choosing the **random** strategy.                                |
| `{"type": "int", "min": 0, "random": true}`        | :arrow_up: Equivalent, explicitly declaring the strategy but inheriting the `min` from the parent. |
| `{"type": "int", "min": 100, random": {"min": 0}}` | :arrow_up: Equivalent, because the `min` argument is ignored from the parent when it's supplied.   |

### `INT` Gaussian distributions

You can explicitly set the **gauss** strategy by including the `gauss` annotation on the type.
This is useful to have numbers that fit in a bell curve. 
The value can either be `true` to use the defaults for this strategy, or it can be a JSON object with the following arguments:

- `mean` to specify the mean value (default: `0`).
- `stddev` to specify the standard deviation (default: `100`).
- `min` to specify the lower bounds (inclusive) of the generated value (default: -2147483648).
- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483647).

This distribution is calculated using floating point numbers then truncated, so it might not behave strictly mathematically correct.
The `min` and `max` values are applied by discarding results outside the interval, so be careful that they are coherent.
Setting a min of `1000` with the other defaults will waste a lot of cycles trying to find a matching number.

If any strategy argument can be found directly in the (parent) annotated schema, it will be used as the default.

If no strategy is explicitly selected and either the `mean` or `stddev` argument exist, the **gauss** strategy is used.

| Schema                                                             | Summary                                                                                                                                                                        |
|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "gauss": true}`                                   | Generates a bell curve of numbers centered around `0`, with 95% of the values between `-200` and `200`.                                                                        |
| `{"type": "int", "gauss": {"mean": 100, "stddev": 10}}`            | Generates numbers centered around `100`, with 95% of the values between `80` and `120`. It's very unlikely but possible that a negative value could occur.                     |
| `{"type": "int", "min": 0, "gauss": {"mean": 100, "stddev": 10}}`  | Generates numbers centered around `100`, with 95% of the values between `80` and `120`, guaranteeing no negative values.                                                       |
| `{"type": "int", "max": 50, "gauss": {"mean": 100, "stddev": 10}}` | :warning: The same bell curve as above, but only numbers < `50`. This is 5 standard deviations from the mean; about one value out of every million generated will be accepted. |

### `INT` Sequences

You can setting the **sequence** strategy by including the `sequence` annotation on the schema.
This generates a sequence of whole numbers with an equal step between them.

The value can either be `true` to use the defaults for this strategy, or it can be a JSON object with the following arguments:

- `min` to specify the lower bounds (inclusive) of the generated value (default: 0).
- `max` to specify the upper bounds (exclusive) of the generated value (default: 2147483646 for `INT`).  
- `step` to specify the distance between generated sequential numbers (default: 1).
- `start` to specify the starting value for the sequence (default: `min` for constant or increasing sequences, but `max` for decreasing sequences).
  If this value is outside the intervale [`min`, `max`), then it is ignored and the default is used.

If the `step` is zero or positive, the sequence starts generating at the `min` value and monotonically grows by that value every time.
When it would pass the `max` value, the sequence restarts at the beginning.
If the `step` is greater than one, any remainder is "wrapped around" so it might not restart exactly at `min`.

If the `step` is negative, the sequence starts generating at the `max` value, etc and monotonically decreases, etc.

A zero `step`, of course, starts and stays at `min` forever.

If no strategy is explicitly selected, none of the preceding strategies are implicitly selected, AND either the `start` or `step` argument exist, the **sequence** strategy is used.

| Schema                                                  | Summary                                                                                                         |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "sequence": true}`                     | `0`, `1`, `2`, `3`, `4`, `5` until it reaches `2147483646`, then restarts.                                      |
| `{"type": "int", "step": 1}`                            | :arrow_up: Equivalent, implicitly choosing the **sequence** strategy.                                           |
| `{"type": "int", "start": 10000}`                       | :arrow_up: `10000`, `10001`, `10002`, `10003`, `10004`, `10005`, implicitly choosing the **sequence** strategy. |
| `{"type": "int", "step": -1, "max": 4}`                 | `3`, `2`, `1`, `0`, `3`, `2`, etc.                                                                              |
| `{"type": "int", "start": 3, "max": 4, "step": 3}`      | :arrow_up: Equivalent because of the wrapped remainder, but confusing.                                          |
| `{"type": "int", "sequence": {"start": 10, "max": 13}}` | `10`, `11`, `12`, `0`, `1`, `2`, etc.                                                                           |
| `{"type": "int", "sequence": {"start": 10, "max": 13}}` | `10`, `11`, `12`, `0`, `1`, `2`, etc.                                                                           |
| `{"type": "int", "max": 13, "sequence": {"start": 10}}` | :arrow_up: Equivalent, inheriting the `max` argument from the parent.                                           |

### `INT` Value

You can explicitly set the **value** strategy, which supplies the datum to return.
- If the value is a JSON number, that number is always returned (simply truncated if floating point).
- If the value is a JSON object, that object is parsed as if it were annotations on the `INT` type, inheriting arguments from the parent, and *that* faker strategy is used.
- If the value is a JSON array, then one of the elements is picked randomly as the value, or used to generate the value.

| Schema                                                               | Summary                                                                                                                              |
|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `{"type": "int", "value": 123}`                                      | Always generates `123`.                                                                                                              |
| `{"type": "int", "value": {"value": 123}}`                           | :arrow_up: Equivalent, but useless.                                                                                                  |
| `{"type": "int", "value": [123, 321]}`                               | Randomly picks `123` or `321`.                                                                                                       |
| `{"type": "int", "value": {"random": {"min": 0}}}`                   | **random** generating a  whole number from 0 to **2147483646**.                                                                      |
| `{"type": "int", "max": 4, "value": {"start": 1, "sequence": true}}` | **sequence** generating `1`, `2`, `3`, `0`, `1`, `2`, etc.   Note that the sequence arguments are inherited up to the parent schema. |
| `{"type": "int", "value": [999, {"random": {"min": 0, "max": 9}}]}`  | Picks `999` 50% of the time, and a single digit number the other 50%.                                                                |

If it is a JSON array, and at the _same_ level as the `value` attribute, there exists an `index` attribute that _is_ a string, then

1. The value of the `index` attribute is used to generate an integer value within the interval [`0`, array size).
   The process is identical to how a `value` attribute is processed (as a constant, a generator strategy, or an array of generator strategies),
   with an implicit `min` and `max` supplied to the strategies.
   If the result happens to be outside the interval, either `min` or `max` is used instead. 
2. The generated integer is used to pick the value in the `value` JSON array that is actually used for the generator.

| Schema                                                                                | Summary                                                                                       |
|---------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `{"type": "int", "value": [123, 321, 999], "index": 0}`                               | Always generates `123`.                                                                       |
| `{"type": "int", "value": [123, 321, 999], "index": -1}`                              | :arrow_up: Equivalent.                                                                        |
| `{"type": "int", "value": [123, 321, 999], "index": {"step": 1}}`                     | The index is a sequence `0`,`1`,`2`,`0`,`1`, etc, so alternates `123`,`321`,`999`,`123`, etc. |
| `{"type": "int", "value": [1,2,3,4,5], "index": {"gauss": {"mean": 2, "stddev": 1}}}` | Picks the numbers `1`,`2`,`3`,`4`,`5` in a rough bell curve.                                  |

If it is a JSON array, and at the _same_ level as the `value` attribute, there exists an `index` attribute that _is_ a string, then special processing occurs:

- If `index` is `sum`, then every argument of the array is used to generate an integer and the sum is returned.  
  Be careful about inheriting `min` and `max`, since they apply to *each* generated integer separately.
- If `index` is `product`, then every argument of the array is used to generate an integer and their product is returned.  
- If `index` is `max`, then every argument of the array is used to generate an integer and the maximum is returned.
- If `index` is `min`, then every argument of the array is used to generate an integer and the minimum is returned.
- If `index` is `mean`, then every argument of the array is used to generate an integer and the average is returned.

| Schema                                                      | Summary                 |
|-------------------------------------------------------------|-------------------------|
| `{"type": "int", "value": [1,2,3,4,5], "index": "sum"}`     | Always generates `15`.  |
| `{"type": "int", "value": [1,2,3,4,5], "index": "product"}` | Always generates `120`. |
| `{"type": "int", "value": [1,2,3,4,5], "index": "min"}`     | Always generates `1`.   |
| `{"type": "int", "value": [1,2,3,4,5], "index": "max"}`     | Always generates `5`.   |
| `{"type": "int", "value": [1,2,3,4,5], "index": "mean"}`    | Always generates `3`.   |

Generating `LONG`, `FLOAT` and `DOUBLE` data
------------------------------------------------------------------------------

The same rules and strategies apply to the other numerical types, using their respective bounds and precision in Java.

Notes:
- The default `min` and `max` for **random** `FLOAT` and `DOUBLE` are `0.0` and `1.0` (not their minimum and maximum values).  Likewise, the default `mean` and `stddev` are `0.0` and `1.0`. 
- The default `min` and `max` for floating point sequences are their minimum and maximum values.
- Where values are truncated for whole numbers, they are retained with as much precision as possible for floating point values.

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
