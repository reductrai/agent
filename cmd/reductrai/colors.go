package main

// ANSI color codes
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorGray   = "\033[90m"
	ColorWhite  = "\033[97m"
	ColorBold   = "\033[1m"
)

// Colored output helpers
func green(s string) string  { return ColorGreen + s + ColorReset }
func red(s string) string    { return ColorRed + s + ColorReset }
func yellow(s string) string { return ColorYellow + s + ColorReset }
func cyan(s string) string   { return ColorCyan + s + ColorReset }
func gray(s string) string   { return ColorGray + s + ColorReset }
func bold(s string) string   { return ColorBold + s + ColorReset }
