package bi.know.kettle.neo4j.model;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.neo4j.kettle.core.Neo4jDefaults;
import org.neo4j.kettle.core.metastore.MetaStoreFactory;
import org.neo4j.kettle.model.GraphModel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.metastore.api.IMetaStore;

public class GraphModelUtils {
  private static Class<?> PKG = GraphModelUtils.class; // for i18n purposes, needed by Translator2!!

  private static MetaStoreFactory<GraphModel> staticFactory;

  public static MetaStoreFactory<GraphModel> getModelFactory( IMetaStore metaStore ) {
    if ( staticFactory == null ) {
      staticFactory = new MetaStoreFactory<>( GraphModel.class, metaStore, Neo4jDefaults.NAMESPACE );
    }
    return staticFactory;
  }

  public static GraphModel newModel( Shell shell, MetaStoreFactory<GraphModel> factory, RowMetaInterface inputRowMeta ) {
    GraphModel graphModel = new GraphModel();
    boolean ok = false;
    while ( !ok ) {
      GraphModelDialog dialog = new GraphModelDialog( shell, graphModel, inputRowMeta );
      if ( dialog.open() ) {

        // write to metastore...
        //
        try {
          if ( factory.loadElement( graphModel.getName() ) != null ) {
            MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "GraphModelUtils.Error.ModelExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "GraphModelUtils.Error.ModelExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( graphModel );
              ok = true;
            }
          } else {
            factory.saveElement( graphModel );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorSavingModel.Title" ),
            BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorSavingModel.Message" ),
            exception );
          return null;
        }
      } else {
        // Cancel
        return null;
      }
    }
    return graphModel;
  }

  public static void editModel( Shell shell, MetaStoreFactory<GraphModel> factory, String modelName, RowMetaInterface inputRowMeta ) {
    if ( StringUtils.isEmpty( modelName ) ) {
      return;
    }
    try {
      GraphModel GraphModel = factory.loadElement( modelName );
      if ( GraphModel == null ) {
        newModel( shell, factory, inputRowMeta );
      } else {
        GraphModelDialog GraphModelDialog = new GraphModelDialog( shell, GraphModel, inputRowMeta );
        if ( GraphModelDialog.open() ) {
          factory.saveElement( GraphModel );
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorEditingModel.Title" ),
        BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorEditingModel.Message" ),
        exception );
    }
  }

  public static void deleteModel( Shell shell, MetaStoreFactory<GraphModel> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }

    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setText( BaseMessages.getString( PKG, "GraphModelUtils.DeleteModelConfirmation.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "GraphModelUtils.DeleteModelConfirmation.Message", connectionName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        factory.deleteElement( connectionName );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorDeletingModel.Title" ),
          BaseMessages.getString( PKG, "GraphModelUtils.Error.ErrorDeletingModel.Message", connectionName ),
          exception );
      }
    }
  }

}