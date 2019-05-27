import * as React from 'react';
import {
    Table, TableHead, TableRow, TableCell, withStyles
} from '@material-ui/core';

interface Props{
    classes?: any;
}
interface State{}

class MessageTable extends React.Component<Props, State> {
    render() {
        const { classes } = this.props;
        return (
            <Table className={classes.table}>
                <TableHead>
                    <TableRow>
                        <TableCell>Id</TableCell>
                        <TableCell>Message</TableCell>
                    </TableRow>
                </TableHead>
            </Table>
        )
    }
}

const styles = {
    table: {
        minWidth: 700
    }
};

export default withStyles(styles)(MessageTable)