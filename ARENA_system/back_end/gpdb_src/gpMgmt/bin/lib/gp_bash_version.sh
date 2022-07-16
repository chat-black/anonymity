gp_version="6.0.0-beta.1+995f61cc build dev"

print_version ()
{
	progname="$(basename $0)"
	echo "${progname} ${gp_version}"
	exit 0
}
