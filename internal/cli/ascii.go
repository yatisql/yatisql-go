package cli

import (
	"fmt"

	"github.com/fatih/color"
)

// PrintASCIIArt prints the YATISQL ASCII art banner.
func PrintASCIIArt() {
	// Only show ASCII art if output is a terminal
	if !isTerminal() {
		return
	}

	// Color for the ASCII art - warm yellow
	bannerColor := color.New(color.FgYellow, color.Bold)
	subtitleColor := color.New(color.FgHiBlack)

	bannerColor.Println("")
	bannerColor.Println("██╗   ██╗ █████╗ ████████╗██╗███████╗ ██████╗ ██╗     ")
	bannerColor.Println("╚██╗ ██╔╝██╔══██╗╚══██╔══╝██║██╔════╝██╔═══██╗██║     ")
	bannerColor.Println(" ╚████╔╝ ███████║   ██║   ██║███████╗██║   ██║██║     ")
	bannerColor.Println("  ╚██╔╝  ██╔══██║   ██║   ██║╚════██║██║▄▄ ██║██║     ")
	bannerColor.Println("   ██║   ██║  ██║   ██║   ██║███████║╚██████╔╝███████╗")
	bannerColor.Println("   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝ ╚══▀▀═╝ ╚══════╝")
	subtitleColor.Println("          Yet Another Tabular Inefficient SQL          ")
	fmt.Println()
}
