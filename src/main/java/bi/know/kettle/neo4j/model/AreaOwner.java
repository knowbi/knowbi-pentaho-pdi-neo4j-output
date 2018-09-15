package bi.know.kettle.neo4j.model;

import java.awt.geom.Area;
import java.util.List;
import java.util.ListIterator;

public class AreaOwner {
  private int x;
  private int y;
  private int width;
  private int height;

  private AreaType areaType;
  private Object subject;

  public AreaOwner() {
  }

  public AreaOwner( int x, int y, int width, int height, AreaType areaType, Object subject ) {
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
    this.areaType = areaType;
    this.subject = subject;
  }

  /**
   * Find the first area matching the given coordinate, starting from the back of the list
   * @param x
   * @param y
   * @return the area owner found or null if nothing matched.
   */
  public static final AreaOwner findArea( List<AreaOwner> areaOwners, int x, int y) {
    ListIterator<AreaOwner> li = areaOwners.listIterator(areaOwners.size());
    while(li.hasPrevious()) {
      AreaOwner areaOwner = li.previous();
      if (areaOwner.contains(x, y)) {
        System.out.println("Match: "+areaOwner.getAreaType()+" on ("+x+", "+y+")");
        return areaOwner;
      }
    }
    return null;
  }

  private boolean contains( int locX, int locY ) {
    // System.out.println("Checking ("+locX+", "+locY+") against ("+x+", "+y+", "+(x+width)+", "+(y+height)+")");

    return locX>=x && locY>=y && locX<=x+width && locY<=y+height;
  }

  /**
   * Gets x
   *
   * @return value of x
   */
  public int getX() {
    return x;
  }

  /**
   * @param x The x to set
   */
  public void setX( int x ) {
    this.x = x;
  }

  /**
   * Gets y
   *
   * @return value of y
   */
  public int getY() {
    return y;
  }

  /**
   * @param y The y to set
   */
  public void setY( int y ) {
    this.y = y;
  }

  /**
   * Gets width
   *
   * @return value of width
   */
  public int getWidth() {
    return width;
  }

  /**
   * @param width The width to set
   */
  public void setWidth( int width ) {
    this.width = width;
  }

  /**
   * Gets height
   *
   * @return value of height
   */
  public int getHeight() {
    return height;
  }

  /**
   * @param height The height to set
   */
  public void setHeight( int height ) {
    this.height = height;
  }

  /**
   * Gets areaType
   *
   * @return value of areaType
   */
  public AreaType getAreaType() {
    return areaType;
  }

  /**
   * @param areaType The areaType to set
   */
  public void setAreaType( AreaType areaType ) {
    this.areaType = areaType;
  }

  /**
   * Gets subject
   *
   * @return value of subject
   */
  public Object getSubject() {
    return subject;
  }

  /**
   * @param subject The subject to set
   */
  public void setSubject( Object subject ) {
    this.subject = subject;
  }
}
