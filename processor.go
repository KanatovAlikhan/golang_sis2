package main

import (
	"fmt"
	"math"
)


type TaskProcessor interface {
	Calculate(numbers []float64) (Statistics, error)
}

type StatisticsProcessor struct{}

func (StatisticsProcessor) Calculate(numbers []float64) (Statistics, error) {
	if err := validateNumbers(numbers); err != nil {
		return Statistics{}, err
	}

	result := Statistics{
		Count: len(numbers),
		Min:   numbers[0],
		Max:   numbers[0],
	}

	for _, number := range numbers {
		result.Sum += number
		if number < result.Min {
			result.Min = number
		}
		if number > result.Max {
			result.Max = number
		}
	}

	if math.IsInf(result.Sum, 0) {
		return Statistics{}, fmt.Errorf("%w: the sum is too large", ErrInvalidInput)
	}
	result.Average = result.Sum / float64(result.Count)
	return result, nil
}
